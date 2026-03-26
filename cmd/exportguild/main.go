package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/TicketsBot-cloud/common/encryption"
	"github.com/TicketsBot-cloud/gdl/objects/channel/message"
	"github.com/TicketsBot-cloud/logarchiver/internal/util"
	"github.com/TicketsBot-cloud/logarchiver/pkg/config"
	"github.com/TicketsBot-cloud/logarchiver/pkg/model"
	v1 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v1"
	v2 "github.com/TicketsBot-cloud/logarchiver/pkg/model/v2"
	"github.com/TicketsBot-cloud/logarchiver/pkg/s3client"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/sync/errgroup"
)

const workers = 15

var (
	guildId       = flag.Uint64("guildid", 0, "guild id to export")
	key           = flag.String("key", "", "aes key")
	ticketId      = flag.Int("ticketid", 0, "set to export a single ticket")
	convert       = flag.Bool("convert", false, "convert to v2 if necessary")
	userWhitelist = flag.Uint64("userwhitelist", 0, "only export tickets from this user")
	after         = flag.Int("after", 0, "export ticket IDs above this value (inclusive)")
)

func main() {
	flag.Parse()
	conf := config.Parse[config.CliConfig]()

	fmt.Printf("Exporting guild data for %d\n", *guildId)

	exportDir := fmt.Sprintf("exports/guild/%d", *guildId)

	if err := os.MkdirAll(exportDir, 0700); err != nil {
		panic(fmt.Sprintf("failed to create export directory: %v", err))
	}

	fmt.Println("[1/3] Connecting to S3...")
	m, err := minio.New(conf.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(conf.AccessKey, conf.SecretKey, ""),
		Secure: conf.Secure,
	})
	if err != nil {
		panic(err)
	}

	client := s3client.NewS3Client(m, conf.Bucket)

	fmt.Println("[2/3] Exporting transcripts...")
	if ticketId != nil && *ticketId > 0 {
		exportTicket(*ticketId, client, exportDir)
		printProgress(1, 1, 0, 0)
	} else {
		keys, err := client.GetAllKeysForGuild(context.Background(), *guildId)
		if err != nil {
			panic(err)
		}

		total := int64(len(keys))
		var processed, skipped, failed atomic.Int64

		keyCh := make(chan string)
		go func() {
			for _, key := range keys {
				keyCh <- key
			}
			close(keyCh)
		}()

		group, _ := errgroup.WithContext(context.Background())
		for i := 0; i < workers; i++ {
			group.Go(func() error {
				for key := range keyCh {
					id := key[strings.LastIndex(key, "/")+1:]
					if id == "" {
						skipped.Add(1)
						processed.Add(1)
						printProgress(int(processed.Load()), int(total), int(skipped.Load()), int(failed.Load()))
						continue
					}

					parsed, err := strconv.Atoi(id)
					if err != nil {
						skipped.Add(1)
						processed.Add(1)
						printProgress(int(processed.Load()), int(total), int(skipped.Load()), int(failed.Load()))
						continue
					}

					if after != nil && *after > 0 && parsed < *after {
						skipped.Add(1)
						processed.Add(1)
						printProgress(int(processed.Load()), int(total), int(skipped.Load()), int(failed.Load()))
						continue
					}

					if ok := exportTicket(parsed, client, exportDir); !ok {
						failed.Add(1)
					}

					processed.Add(1)
					printProgress(int(processed.Load()), int(total), int(skipped.Load()), int(failed.Load()))
				}

				return nil
			})
		}

		if err := group.Wait(); err != nil {
			panic(err)
		}
	}

	fmt.Println("[3/3] Creating zip archive...")
	zipPath := fmt.Sprintf("exports/guild/%d.zip", *guildId)
	if err := util.ZipFiles(exportDir, zipPath); err != nil {
		panic(fmt.Sprintf("could not zip files: %v", err))
	}

	os.RemoveAll(exportDir)
	fmt.Printf("Done! Export saved to %s\n", zipPath)
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

// exportTicket exports a single ticket and returns true on success.
func exportTicket(id int, client *s3client.S3Client, exportDir string) bool {
	data, err := client.GetTicket(context.Background(), *guildId, id)
	if err != nil {
		return false
	}

	data, err = encryption.Decrypt([]byte(*key), data)
	if err != nil {
		return false
	}

	if *convert || (userWhitelist != nil && *userWhitelist > 0) {
		var transcript v2.Transcript

		version := model.GetVersion(data)
		switch version {
		case model.V1:
			var messages []message.Message
			if err := json.Unmarshal(data, &messages); err != nil {
				return false
			}

			transcript = v1.ConvertToV2(messages)
		case model.V2:
			if err := json.Unmarshal(data, &transcript); err != nil {
				return false
			}
		default:
			return false
		}

		if userWhitelist != nil && *userWhitelist > 0 {
			transcript.Entities.Channels = nil
			transcript.Entities.Roles = nil

			user, ok := transcript.Entities.Users[*userWhitelist]
			if !ok {
				transcript.Entities.Users = nil
			} else {
				transcript.Entities.Users = map[uint64]v2.User{
					user.Id: user,
				}
			}

			var messages []v2.Message
			for _, message := range transcript.Messages {
				if message.AuthorId == *userWhitelist {
					messages = append(messages, message)
				}
			}

			transcript.Messages = messages
		}

		data, err = json.Marshal(transcript)
		if err != nil {
			return false
		}
	}

	var encoded bytes.Buffer
	if err := json.Indent(&encoded, data, "", "  "); err != nil {
		return false
	}

	f, err := os.Create(fmt.Sprintf("%s/%d.json", exportDir, id))
	if err != nil {
		return false
	}

	if _, err := encoded.WriteTo(f); err != nil {
		return false
	}

	return true
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
