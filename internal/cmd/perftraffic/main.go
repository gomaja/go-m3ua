package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	config, err := parseConfig(os.Args[1:])
	if err != nil {
		writeCommandFailure(err)
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if config.Role == "sgp" {
		record, runErr := runReceiver(ctx, config)
		if runErr != nil && record.FatalError == "" {
			record.FatalError = runErr.Error()
			record.evaluate()
		}
		_ = encoder.Encode(record)
		if runErr != nil {
			os.Exit(1)
		}
		return
	}
	result, runErr := runSender(ctx, config)
	if runErr != nil && result.Error == "" {
		result.Verdict = verdictInvalid
		result.Error = runErr.Error()
	}
	_ = encoder.Encode(result)
	if runErr != nil {
		os.Exit(1)
	}
}

func writeCommandFailure(err error) {
	_ = json.NewEncoder(os.Stderr).Encode(map[string]any{
		"verdict": verdictInvalid,
		"error":   fmt.Sprintf("%v", err),
	})
}
