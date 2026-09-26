package main

import (
	"context"
	"flag"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/TicketsBot-cloud/logarchiver/pkg/config"
	"github.com/TicketsBot-cloud/logarchiver/pkg/repository"
	"github.com/TicketsBot-cloud/logarchiver/pkg/s3client"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"

	_ "github.com/joho/godotenv/autoload"
)

const (
	workers                = 10
	maxConsecutiveFailures = 20
)

// $2 is the run's start time, as re-closing a reopened thread re-uploads its transcript
const flaggedFilter = `has_transcript = 't' AND open = 'f' AND (close_time IS NULL OR close_time < $2)`

var (
	dbUri     = flag.String("dburi", "", "main database uri")
	guildIds  = flag.String("guildid", "", "guild ID(s) to check, comma separated (default: all)")
	ticketIds = flag.String("ticket", "", "ticket ID(s) to check, comma separated (requires a single -guildid)")
	csvFile   = flag.String("csv", "", "file of guild_id,ticket_id lines to check")
	before    = flag.Uint64("before", 0, "only check guild IDs below this value")
	after     = flag.Uint64("after", 0, "only check guild IDs above this value")
	dryRun    = flag.Bool("dry-run", false, "only print the tickets that would be fixed")
)

func main() {
	flag.Parse()
	conf := config.Parse[config.Config]()

	if flag.NArg() > 0 {
		panic(fmt.Sprintf("unexpected arguments: %q", flag.Args()))
	}

	if *dbUri == "" {
		panic("-dburi is required")
	}

	ctx := context.Background()

	store, err := repository.ConnectPostgres(ctx, conf)
	must(err)

	manager := s3client.NewShardedClientManager(conf, store)
	must(manager.Load(ctx))

	clients := manager.GetAll()
	if len(clients) == 0 {
		panic("no buckets found")
	}

	for _, client := range clients {
		logf("Using bucket %s", client.BucketName())
	}

	pool, err := pgxpool.Connect(ctx, *dbUri)
	if err != nil {
		panic(fmt.Sprintf("failed to connect to database: %v", err))
	}
	defer pool.Close()

	var startedAt time.Time
	must(pool.QueryRow(ctx, `SELECT NOW();`).Scan(&startedAt))

	checkRecentTranscripts(ctx, pool, clients)

	guilds, tickets := getTargets(ctx, pool)
	logf("Checking %d guilds", len(guilds))

	stopCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A piped stdout (e.g. tee) also dies on Ctrl-C, and SIGPIPE would then kill in-flight workers
	signal.Ignore(syscall.SIGPIPE)

	var (
		group                        errgroup.Group
		mu                           sync.Mutex
		failed                       []string
		checked, fixed, failedInARow atomic.Int64
		lastDispatched               = *after
		stoppedEarly                 bool
	)
	group.SetLimit(workers)

	for _, guildId := range guilds {
		if stopCtx.Err() != nil || failedInARow.Load() >= maxConsecutiveFailures {
			logf("Stopping")
			stoppedEarly = true
			// Only here, so a Ctrl-C during the final drain can't kill in-flight workers
			stop()
			break
		}

		group.Go(func() error {
			ids, err := checkGuild(ctx, pool, clients, guildId, startedAt, tickets[guildId])
			if err != nil {
				logf("failed guild %d: %v", guildId, err)
				failedInARow.Add(1)

				mu.Lock()
				failed = append(failed, strconv.FormatUint(guildId, 10))
				mu.Unlock()
			} else {
				failedInARow.Store(0)
			}

			for _, id := range ids {
				if _, err := fmt.Printf("%d,%d\n", guildId, id); err != nil {
					logf("failed to write %d,%d to stdout: %v", guildId, id, err)
				}
			}

			fixed.Add(int64(len(ids)))
			if n := checked.Add(1); n%1000 == 0 {
				logf("Checked %d/%d guilds (%d tickets)", n, len(guilds), fixed.Load())
			}

			return nil
		})

		lastDispatched = guildId
	}

	group.Wait()

	verb := "fixed"
	if *dryRun {
		verb = "would be fixed"
	}

	logf("Done! %d tickets %s (%d guilds failed)", fixed.Load(), verb, len(failed))

	if len(failed) > 0 {
		if len(tickets) > 0 {
			logf("Retry with the same command")
		} else {
			logf("Retry with -guildid=%s", strings.Join(failed, ","))
		}
	}

	if stoppedEarly {
		logf("Stopped early, resume with -after=%d", lastDispatched)
	}
}

func checkRecentTranscripts(ctx context.Context, pool *pgxpool.Pool, clients []*s3client.S3Client) {
	rows, err := pool.Query(ctx, `SELECT guild_id, id FROM tickets WHERE has_transcript = 't' AND open = 'f' AND close_time IS NOT NULL ORDER BY close_time DESC LIMIT 20;`)
	if err != nil {
		panic(fmt.Sprintf("failed to query recent tickets: %v", err))
	}

	type ticket struct {
		guildId uint64
		id      int
	}

	var tickets []ticket
	for rows.Next() {
		var t ticket
		must(rows.Scan(&t.guildId, &t.id))
		tickets = append(tickets, t)
	}

	rows.Close()
	must(rows.Err())

	var found int
	for _, t := range tickets {
		ok, err := exists(ctx, clients, t.guildId, t.id)
		if err != nil {
			panic(fmt.Sprintf("failed to check %d/%d: %v", t.guildId, t.id, err))
		}

		if ok {
			found++
		}
	}

	logf("Found %d/%d recent transcripts", found, len(tickets))

	if found*2 < len(tickets) {
		panic(fmt.Sprintf("only %d/%d recent transcripts found, check the archiver env", found, len(tickets)))
	}
}

func exists(ctx context.Context, clients []*s3client.S3Client, guildId uint64, ticketId int) (bool, error) {
	key := fmt.Sprintf("%d/%d", guildId, ticketId)

	for _, client := range clients {
		found := false
		for obj := range client.Minio().ListObjects(ctx, client.BucketName(), minio.ListObjectsOptions{Prefix: key}) {
			if obj.Err != nil {
				return false, obj.Err
			}

			if obj.Key == key {
				found = true
			}
		}

		if found {
			return true, nil
		}
	}

	return false, nil
}

func getTargets(ctx context.Context, pool *pgxpool.Pool) ([]uint64, map[uint64][]int) {
	tickets := make(map[uint64][]int)

	switch {
	case *csvFile != "":
		if *guildIds != "" || *ticketIds != "" {
			panic("-csv can't be combined with -guildid or -ticket")
		}

		data, err := os.ReadFile(*csvFile)
		must(err)

		for i, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			rawGuildId, rawTicketId, _ := strings.Cut(line, ",")
			guildId, guildErr := strconv.ParseUint(rawGuildId, 10, 64)
			ticketId, ticketErr := strconv.Atoi(rawTicketId)
			if guildErr != nil || ticketErr != nil {
				panic(fmt.Sprintf("invalid line %d in %s: %q", i+1, *csvFile, line))
			}

			tickets[guildId] = append(tickets[guildId], ticketId)
		}

		if len(tickets) == 0 {
			panic(fmt.Sprintf("no tickets in %s", *csvFile))
		}
	case *ticketIds != "":
		guildId, err := strconv.ParseUint(strings.TrimSpace(*guildIds), 10, 64)
		if err != nil {
			panic("-ticket requires a single -guildid")
		}

		for _, raw := range strings.Split(*ticketIds, ",") {
			id, err := strconv.Atoi(strings.TrimSpace(raw))
			must(err)
			tickets[guildId] = append(tickets[guildId], id)
		}
	}

	if len(tickets) > 0 {
		return slices.DeleteFunc(slices.Sorted(maps.Keys(tickets)), func(id uint64) bool { return !inRange(id) }), tickets
	}

	var ids []uint64

	if *guildIds != "" {
		for _, raw := range strings.Split(*guildIds, ",") {
			id, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
			must(err)

			if inRange(id) {
				ids = append(ids, id)
			}
		}

		slices.Sort(ids)
		return slices.Compact(ids), nil
	}

	query := `SELECT DISTINCT guild_id FROM tickets WHERE has_transcript = 't' AND open = 'f' AND guild_id > $1`
	args := []interface{}{*after}

	if *before > 0 {
		query += ` AND guild_id < $2`
		args = append(args, *before)
	}

	query += ` ORDER BY guild_id;`

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		panic(fmt.Sprintf("failed to query guilds: %v", err))
	}
	defer rows.Close()

	for rows.Next() {
		var id uint64
		must(rows.Scan(&id))
		ids = append(ids, id)
	}

	must(rows.Err())
	return ids, nil
}

func inRange(guildId uint64) bool {
	return guildId > *after && (*before == 0 || guildId < *before)
}

func checkGuild(ctx context.Context, pool *pgxpool.Pool, clients []*s3client.S3Client, guildId uint64, startedAt time.Time, only []int) ([]int, error) {
	query := `SELECT id FROM tickets WHERE guild_id = $1 AND ` + flaggedFilter
	args := []interface{}{guildId, startedAt}

	if len(only) > 0 {
		query += ` AND id = ANY($3)`
		args = append(args, only)
	}

	flagged, err := queryIds(ctx, pool, query+`;`, args...)
	if err != nil || len(flagged) == 0 {
		return nil, err
	}

	// Listed twice, as a re-upload deletes the old object before storing the new one
	missing := flagged
	listedAt := time.Now()
	for pass := range 2 {
		if pass == 1 {
			time.Sleep(time.Until(listedAt.Add(5 * time.Second)))
		}

		present := make(map[int]bool)
		for _, client := range clients {
			keys, err := client.GetAllKeysForGuild(ctx, guildId)
			if err != nil {
				return nil, err
			}

			for _, key := range keys {
				// pre-2024 free tickets were stored as {guild}/free-{id}
				if id, err := strconv.Atoi(strings.TrimPrefix(key[strings.LastIndex(key, "/")+1:], "free-")); err == nil {
					present[id] = true
				}
			}
		}

		missing = slices.DeleteFunc(missing, func(id int) bool { return present[id] })
		if len(missing) == 0 {
			return nil, nil
		}
	}

	if *dryRun {
		return missing, nil
	}

	fixed, err := queryIds(ctx, pool, `UPDATE tickets SET has_transcript = 'f' WHERE guild_id = $1 AND `+flaggedFilter+` AND id = ANY($3) RETURNING id;`, guildId, startedAt, missing)
	if err != nil {
		return nil, fmt.Errorf("update may have applied to tickets %v: %w", missing, err)
	}

	return fixed, nil
}

func queryIds(ctx context.Context, pool *pgxpool.Pool, query string, args ...interface{}) ([]int, error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}

		ids = append(ids, id)
	}

	return ids, rows.Err()
}

func logf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
