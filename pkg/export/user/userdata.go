package user

import (
	"context"
	"fmt"
	"time"

	"github.com/TicketsBot-cloud/database"
)

func GetUserData(db *database.Database, userId uint64) (map[string]interface{}, error) {
	ctx := context.Background()
	data := make(map[string]interface{})

	blacklistedGuilds, err := getBlacklistedGuilds(db, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get blacklisted guilds: %w", err)
	}
	data["blacklisted_guilds"] = blacklistedGuilds

	closeRequests, err := getCloseRequests(db, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get close requests: %w", err)
	}
	data["close_requests"] = closeRequests

	responseTimes, err := getResponseTimes(db, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get response times: %w", err)
	}
	data["response_times"] = responseTimes

	participatedTickets, err := getParticipatedTickets(db, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get participated tickets: %w", err)
	}
	data["participated_tickets"] = participatedTickets

	permissions, err := getPermissions(db, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get permissions: %w", err)
	}
	data["permissions"] = permissions

	teamPermissions, err := getTeamPermissions(db, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get team permissions: %w", err)
	}
	data["team_permissions"] = teamPermissions

	claimedTickets, err := getClaimedTickets(db, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get claimed tickets: %w", err)
	}
	data["claimed_tickets"] = claimedTickets

	ticketsMember, err := getTicketsMember(db, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get tickets member: %w", err)
	}
	data["member_of_tickets"] = ticketsMember

	tickets, err := getTickets(db, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get tickets: %w", err)
	}
	data["tickets"] = tickets

	premiumActivatedFor, err := getPremiumActivatedFor(db, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get premium activated for: %w", err)
	}
	data["premium_activated_for"] = premiumActivatedFor

	guilds, err := db.UserGuilds.Get(ctx, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get user guilds: %w", err)
	}
	data["guilds"] = guilds

	whitelabel, err := db.Whitelabel.GetByUserId(ctx, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get whitelabel: %w", err)
	}
	if whitelabel.UserId == 0 {
		data["whitelabel"] = nil
	} else {
		data["whitelabel"] = whitelabel
	}

	whitelabelExpiry, err := db.WhitelabelUsers.GetExpiry(ctx, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get whitelabel expiry: %w", err)
	}
	if whitelabelExpiry.IsZero() {
		data["whitelabel_expiry"] = nil
	} else {
		data["whitelabel_expiry"] = whitelabelExpiry
	}

	return data, nil
}

func getBlacklistedGuilds(db *database.Database, userId uint64) ([]uint64, error) {
	rows, err := db.Blacklist.Query(context.Background(), "SELECT guild_id FROM blacklist WHERE user_id = $1;", userId)
	if err != nil {
		return nil, err
	}

	var guilds []uint64
	for rows.Next() {
		var guildId uint64
		if err := rows.Scan(&guildId); err != nil {
			return nil, err
		}

		guilds = append(guilds, guildId)
	}

	return guilds, nil
}

func getCloseRequests(db *database.Database, userId uint64) ([]database.CloseRequest, error) {
	query := `
SELECT "guild_id", "ticket_id", "user_id", "close_at", "close_reason"
FROM close_request
WHERE "user_id" = $1;`

	rows, err := db.Blacklist.Query(context.Background(), query, userId)
	if err != nil {
		return nil, err
	}

	var requests []database.CloseRequest
	for rows.Next() {
		var request database.CloseRequest
		if err := rows.Scan(&request.GuildId, &request.TicketId, &request.UserId, &request.CloseAt, &request.Reason); err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}

	return requests, nil
}

func getResponseTimes(db *database.Database, userId uint64) ([]interface{}, error) {
	query := `
SELECT "guild_id", "ticket_id", "user_id", "response_time"
FROM first_response_time
WHERE "user_id" = $1;`

	rows, err := db.Blacklist.Query(context.Background(), query, userId)
	if err != nil {
		return nil, err
	}

	var times []interface{}
	for rows.Next() {
		var guildId, userId uint64
		var ticketId int
		var responseTime time.Duration

		if err := rows.Scan(&guildId, &ticketId, &userId, &responseTime); err != nil {
			return nil, err
		}
		times = append(times, map[string]interface{}{
			"guild_id":      guildId,
			"ticket_id":     ticketId,
			"user_id":       userId,
			"response_time": responseTime,
		})
	}

	return times, nil
}

func getParticipatedTickets(db *database.Database, userId uint64) ([]string, error) {
	query := `
SELECT "guild_id", "ticket_id"
FROM participant
WHERE "user_id" = $1;`

	rows, err := db.Blacklist.Query(context.Background(), query, userId)
	if err != nil {
		return nil, err
	}

	var tickets []string
	for rows.Next() {
		var guildId uint64
		var ticketId int

		if err := rows.Scan(&guildId, &ticketId); err != nil {
			return nil, err
		}
		tickets = append(tickets, fmt.Sprintf("%d/%d", guildId, ticketId))
	}

	return tickets, nil
}

func getPermissions(db *database.Database, userId uint64) (map[uint64]string, error) {
	query := `
SELECT "guild_id", "support", "admin"
FROM permissions
WHERE "user_id" = $1;`

	rows, err := db.Blacklist.Query(context.Background(), query, userId)
	if err != nil {
		return nil, err
	}

	data := make(map[uint64]string)
	for rows.Next() {
		var guildId uint64
		var isSupport, isAdmin bool

		if err := rows.Scan(&guildId, &isSupport, &isAdmin); err != nil {
			return nil, err
		}

		if isAdmin {
			data[guildId] = "admin"
		} else if isSupport {
			data[guildId] = "support"
		} else {
			data[guildId] = "none"
		}
	}

	return data, nil
}

func getTeamPermissions(db *database.Database, userId uint64) ([]int, error) {
	query := `
SELECT "team_id"
FROM support_team_members
WHERE "user_id" = $1;`

	rows, err := db.Blacklist.Query(context.Background(), query, userId)
	if err != nil {
		return nil, err
	}

	var teams []int
	for rows.Next() {
		var teamId int
		if err := rows.Scan(&teamId); err != nil {
			return nil, err
		}
		teams = append(teams, teamId)
	}

	return teams, nil
}

func getClaimedTickets(db *database.Database, userId uint64) ([]string, error) {
	query := `
SELECT "guild_id", "ticket_id"
FROM ticket_claims
WHERE "user_id" = $1;`

	rows, err := db.Blacklist.Query(context.Background(), query, userId)
	if err != nil {
		return nil, err
	}

	var tickets []string
	for rows.Next() {
		var guildId uint64
		var ticketId int

		if err := rows.Scan(&guildId, &ticketId); err != nil {
			return nil, err
		}
		tickets = append(tickets, fmt.Sprintf("%d/%d", guildId, ticketId))
	}

	return tickets, nil
}

func getTicketsMember(db *database.Database, userId uint64) ([]string, error) {
	query := `
SELECT "guild_id", "ticket_id"
FROM ticket_members
WHERE "user_id" = $1;`

	rows, err := db.Blacklist.Query(context.Background(), query, userId)
	if err != nil {
		return nil, err
	}

	var tickets []string
	for rows.Next() {
		var guildId uint64
		var ticketId int

		if err := rows.Scan(&guildId, &ticketId); err != nil {
			return nil, err
		}
		tickets = append(tickets, fmt.Sprintf("%d/%d", guildId, ticketId))
	}

	return tickets, nil
}

func getTickets(db *database.Database, userId uint64) ([]database.Ticket, error) {
	query := `
SELECT id, guild_id, channel_id, user_id, open, open_time, welcome_message_id, panel_id, has_transcript
FROM tickets
WHERE "user_id" = $1;`

	rows, err := db.Blacklist.Query(context.Background(), query, userId)
	if err != nil {
		return nil, err
	}

	var tickets []database.Ticket
	for rows.Next() {
		var ticket database.Ticket
		if err := rows.Scan(&ticket.Id, &ticket.GuildId, &ticket.ChannelId, &ticket.UserId, &ticket.Open, &ticket.OpenTime, &ticket.WelcomeMessageId, &ticket.PanelId, &ticket.HasTranscript); err != nil {
			return nil, err
		}
		tickets = append(tickets, ticket)
	}

	return tickets, nil
}

func getPremiumActivatedFor(db *database.Database, userId uint64) ([]uint64, error) {
	query := `
SELECT guild_id
FROM used_keys
WHERE "activated_by" = $1;`

	rows, err := db.Blacklist.Query(context.Background(), query, userId)
	if err != nil {
		return nil, err
	}

	var guilds []uint64
	for rows.Next() {
		var guildId uint64
		if err := rows.Scan(&guildId); err != nil {
			return nil, err
		}
		guilds = append(guilds, guildId)
	}

	return guilds, nil
}
