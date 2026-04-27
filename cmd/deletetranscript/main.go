package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/TicketsBot-cloud/logarchiver/pkg/config"
	"github.com/TicketsBot-cloud/logarchiver/pkg/s3client"

	_ "github.com/joho/godotenv/autoload"
)

var (
	guildId  = flag.Uint64("guildid", 0, "guild ID the ticket is from")
	ticketId = flag.String("ticket", "", "ticket ID(s) to clean")
	all      = flag.Bool("all", false, "apply to all tickets")
)

func main() {
	flag.Parse()
	cfg := config.Parse[config.CliConfig]()

	if guildId == nil || *guildId == 0 {
		panic("guild id must be set")
	}

	client, err := s3client.NewFromCliConfig(cfg)
	if err != nil {
		panic(err)
	}

	if *all {
		keys, err := client.GetAllKeysForGuild(context.Background(), *guildId)
		if err != nil {
			panic(err)
		}

		for _, key := range keys {
			ticketId, err := s3client.TicketIDFromKey(key)
			if err != nil {
				fmt.Printf("error occurred while parsing id of %s: %v\n", key, err)
				continue
			}

			must(client.DeleteTicket(context.Background(), *guildId, ticketId))

			fmt.Printf("cleaned %d\n", ticketId)
		}
	} else if *ticketId != "" {
		split := strings.Split(*ticketId, ",")

		for _, id := range split {
			parsed, err := strconv.Atoi(id)
			if err != nil {
				fmt.Printf("error occurred while parsing id of %s: %v\n", id, err)
				continue
			}

			must(client.DeleteTicket(context.Background(), *guildId, parsed))
			fmt.Printf("deleted %d\n", parsed)
		}
	} else {
		panic("all or ticket flag must be set")
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
