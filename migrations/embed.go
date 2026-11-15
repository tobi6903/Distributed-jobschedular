// Package migrations embeds the SQL migration files so binaries are self-contained
// (the migrate Job in Kubernetes needs no volume mount).
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
