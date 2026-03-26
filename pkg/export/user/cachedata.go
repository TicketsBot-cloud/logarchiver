package user

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/TicketsBot-cloud/gdl/cache"
)

func GetCacheData(c *cache.PgCache, userId uint64) (map[string]interface{}, error) {
	data := make(map[string]interface{})

	user, err := c.GetUser(context.Background(), userId)
	if err == nil {
		data["user"] = user
	} else {
		data["user"] = nil
	}

	rows, err := c.Query(context.Background(), `SELECT guild_id, data FROM members WHERE "user_id" = $1;`, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to query member data: %w", err)
	}

	memberData := make(map[string]interface{})
	for rows.Next() {
		var guildId uint64
		var raw string

		if err := rows.Scan(&guildId, &raw); err != nil {
			return nil, fmt.Errorf("failed to scan member row: %w", err)
		}

		memberData[strconv.FormatUint(guildId, 10)] = json.RawMessage([]byte(raw))
	}

	data["member_data"] = memberData

	return data, nil
}
