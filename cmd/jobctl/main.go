// Command jobctl is the operator CLI: run migrations, mint API keys, and talk to the API.
//
//	jobctl migrate up|down [n]|version      (needs DATABASE_URL)
//	jobctl create-key -tenant T [-name N]   (needs DATABASE_URL; prints the key once)
//	jobctl submit -type T [flags]           (needs JOBCTL_API_KEY, optional JOBCTL_URL)
//	jobctl get [-wait] ID
//	jobctl replay ID
//	jobctl schedule create|list|get|pause|resume|delete ...
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
