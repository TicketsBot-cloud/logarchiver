package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/TicketsBot-cloud/common/encryption"
	"github.com/TicketsBot-cloud/database"
	"github.com/TicketsBot-cloud/gdl/cache"
	"github.com/TicketsBot-cloud/gdl/objects/channel/message"
	"github.com/TicketsBot-cloud/logarchiver/internal/util"
	"github.com/TicketsBot-cloud/logarchiver/pkg/config"
	"github.com/TicketsBot-cloud/logarchiver/pkg/export/user"
	"github.com/TicketsBot-cloud/logarchiver/pkg/model"
	v1 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v1"
	v2 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v2"
	"github.com/TicketsBot-cloud/logarchiver/pkg/s3client"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	userId   = flag.Uint64("userid", 0, "user id to export")
	key      = flag.String("key", "", "aes key")
	dbUri    = flag.String("dburi", "", "database uri")
	cacheUri = flag.String("cacheuri", "", "cache uri")
)

func main() {
	flag.Parse()
	conf := config.Parse[config.CliConfig]()

	exportDir := fmt.Sprintf("exports/user/%d", *userId)

	if err := os.MkdirAll(exportDir, 0700); err != nil {
		panic(fmt.Sprintf("failed to create export directory: %v", err))
	}

	fmt.Printf("Exporting user data for %d\n", *userId)

	fmt.Println("[1/5] Connecting to database...")
	var db *database.Database
	{
		pool, err := pgxpool.Connect(context.Background(), *dbUri)
		must(err)

		db = database.NewDatabase(pool)
	}

	fmt.Println("[2/5] Connecting to cache...")
	var c cache.PgCache
	{
		pool, err := pgxpool.Connect(context.Background(), *cacheUri)
		must(err)

		c = cache.NewPgCache(pool, cache.CacheOptions{
			Users:   true,
			Members: true,
		})
	}

	fmt.Println("[3/5] Exporting database and cache data...")
	{
		userData, err := user.GetUserData(db, *userId)
		must(err)

		encoded, err := json.MarshalIndent(userData, "", "  ")
		must(err)

		writeFile(fmt.Sprintf("%s/database.json", exportDir), encoded)
		fmt.Println("  - Wrote database.json")
	}

	{
		cacheData, err := user.GetCacheData(&c, *userId)
		must(err)

		encoded, err := json.MarshalIndent(cacheData, "", "  ")
		must(err)

		writeFile(fmt.Sprintf("%s/cache.json", exportDir), encoded)
		fmt.Println("  - Wrote cache.json")
	}

	fmt.Println("[4/5] Exporting transcripts...")
	transcriptIds := make(map[uint64][]int)

	{
		query := `SELECT participant.guild_id, participant.ticket_id FROM participant INNER JOIN tickets ON participant.guild_id = tickets.guild_id AND tickets.id = participant.ticket_id WHERE participant.user_id = $1 AND tickets.has_transcript='t' and tickets.open='f';`
		rows, err := db.Participants.Query(context.Background(), query, *userId)
		must(err)

		for rows.Next() {
			var guildId uint64
			var ticketId int

			must(rows.Scan(&guildId, &ticketId))

			if transcriptIds[guildId] == nil {
				transcriptIds[guildId] = make([]int, 0)
			}

			transcriptIds[guildId] = append(transcriptIds[guildId], ticketId)
		}
	}

	{
		query := `SELECT guild_id, id FROM tickets WHERE user_id = $1 AND has_transcript='t' AND open='f';`
		rows, err := db.Tickets.Query(context.Background(), query, *userId)
		must(err)

		for rows.Next() {
			var guildId uint64
			var ticketId int

			must(rows.Scan(&guildId, &ticketId))

			if transcriptIds[guildId] == nil {
				transcriptIds[guildId] = make([]int, 0)
			}

			transcriptIds[guildId] = append(transcriptIds[guildId], ticketId)
		}
	}

	getTranscripts(conf, exportDir, transcriptIds)

	fmt.Println("[5/5] Creating zip archive...")
	if err := util.ZipFiles(exportDir, fmt.Sprintf("exports/user/%d.zip", *userId)); err != nil {
		panic(fmt.Sprintf("could not zip files: %v", err))
	}

	os.RemoveAll(exportDir)
	fmt.Printf("Done! Export saved to exports/user/%d.zip\n", *userId)
}

func printProgress(current, total, skipped, failed int) {
	const barWidth = 40
	filled := barWidth * current / total
	bar := make([]byte, barWidth)
	for i := range bar {
		if i < filled {
			bar[i] = '#'
		} else {
			bar[i] = '-'
		}
	}
	fmt.Printf("\r[%s] %d/%d transcripts (skipped: %d, failed: %d)", bar, current, total, skipped, failed)
	if current == total {
		fmt.Println()
	}
}

func getTranscripts(conf config.CliConfig, exportDir string, tickets map[uint64][]int) {
	m, err := minio.New(conf.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(conf.AccessKey, conf.SecretKey, ""),
		Secure: conf.Secure,
	})
	if err != nil {
		panic(err)
	}

	client := s3client.NewS3Client(m, conf.Bucket)

	_ = os.MkdirAll(fmt.Sprintf("%s/transcripts", exportDir), 0700)

	var total int
	for _, ticketIds := range tickets {
		total += len(ticketIds)
	}

	var processed, skipped, failed int
	for guildId, ticketIds := range tickets {
		for _, ticketId := range ticketIds {
			processed++
			printProgress(processed, total, skipped, failed)

			data, err := client.GetTicket(context.Background(), guildId, ticketId)
			if err != nil {
				if errors.Is(err, s3client.ErrTicketNotFound) || err.Error() == "The specified key does not exist." {
					skipped++
					continue
				} else {
					panic(err)
				}
			}

			data, err = encryption.Decrypt([]byte(*key), data)
			if err != nil {
				failed++
				continue
			}

			// Convert to v2 if needed
			var transcript v2.Transcript

			version := model.GetVersion(data)
			switch version {
			case model.V1:
				var messages []message.Message
				if err := json.Unmarshal(data, &messages); err != nil {
					panic(err)
				}

				transcript = v1.ConvertToV2(messages)
			case model.V2:
				if err := json.Unmarshal(data, &transcript); err != nil {
					panic(err)
				}
			default:
				panic(fmt.Sprintf("Unknown version %d", version))
			}

			transcript.Entities.Channels = nil
			transcript.Entities.Roles = nil

			user, ok := transcript.Entities.Users[*userId]
			if !ok {
				transcript.Entities.Users = nil
			} else {
				transcript.Entities.Users = map[uint64]v2.User{
					user.Id: user,
				}
			}

			var messages []v2.Message
			for _, message := range transcript.Messages {
				if message.AuthorId == *userId {
					messages = append(messages, message)
				}
			}

			transcript.Messages = messages

			encoded, err := json.MarshalIndent(transcript, "", "  ")
			must(err)

			fileName := fmt.Sprintf("%s/transcripts/%d-%d.json", exportDir, guildId, ticketId)
			must(os.WriteFile(fileName, encoded, 0644))
		}
	}
}

func writeFile(fileName string, data []byte) {
	f, err := os.Create(fileName)
	must(err)

	_, err = f.Write(data)
	must(err)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
