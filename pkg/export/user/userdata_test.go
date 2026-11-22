package user

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/TicketsBot-cloud/database"
)

func TestWhitelabelExportOmitsCredentials(t *testing.T) {
	token := "legacy-plaintext-token"
	version := int16(2)

	tests := []struct {
		name string
		bot  database.WhitelabelBot
	}{
		{"plaintext", database.WhitelabelBot{UserId: 1, BotId: 2, PublicKey: "pk", Token: &token}},
		{"ciphertext", database.WhitelabelBot{UserId: 1, BotId: 2, PublicKey: "pk", TokenCiphertext: []byte("ciphertext-bytes"), TokenKeyVersion: &version}},
		{"both", database.WhitelabelBot{UserId: 1, BotId: 2, PublicKey: "pk", Token: &token, TokenCiphertext: []byte("ciphertext-bytes"), TokenKeyVersion: &version}},
		{"zero", database.WhitelabelBot{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Both the export type and the raw table type must be safe.
			for name, v := range map[string]any{"export": newWhitelabelExport(tt.bot), "table": tt.bot} {
				raw, err := json.Marshal(map[string]any{"whitelabel": v})
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}

				out := strings.ToLower(string(raw))
				for _, forbidden := range []string{"token", "ciphertext", "legacy-plaintext-token", "y2lwagvydgv4dc1ieXRlcw"} {
					if strings.Contains(out, forbidden) {
						t.Errorf("%s: export contains %q: %s", name, forbidden, raw)
					}
				}
			}
		})
	}
}
