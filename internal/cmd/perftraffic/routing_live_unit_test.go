package main

import (
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestRoutingLiveHTTPServerOwnerClosesAndJoins(testContext *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		testContext.Fatal(err)
	}
	server := &http.Server{
		Handler:           http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		ReadHeaderTimeout: routingControlTimeout,
		ReadTimeout:       routingControlTimeout,
		WriteTimeout:      routingControlTimeout,
		IdleTimeout:       routingControlTimeout,
	}
	owner := startRoutingLiveHTTPServer(server, listener)
	if err := owner.Close(); err != nil {
		testContext.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		testContext.Fatalf("repeated close: %v", err)
	}
}

func TestRoutingLiveWriteAtomicRefusesOverwrite(testContext *testing.T) {
	directory := testContext.TempDir()
	path := filepath.Join(directory, "result.json")
	if err := routingLiveWriteAtomic(path, []byte("first\n")); err != nil {
		testContext.Fatal(err)
	}
	if err := routingLiveWriteAtomic(path, []byte("second\n")); err == nil {
		testContext.Fatal("overwrite succeeded")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		testContext.Fatal(err)
	}
	if string(contents) != "first\n" {
		testContext.Fatalf("contents=%q", contents)
	}
	info, err := os.Stat(path)
	if err != nil {
		testContext.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		testContext.Fatalf("mode=%#o", info.Mode().Perm())
	}
	matches, err := filepath.Glob(filepath.Join(directory, ".routing-live-*"))
	if err != nil {
		testContext.Fatal(err)
	}
	if len(matches) != 0 {
		testContext.Fatalf("temporary files remain: %v", matches)
	}
}

func TestRoutingLiveWriteAtomicFailsClosedOnUnreadableDestination(testContext *testing.T) {
	directory := testContext.TempDir()
	path := filepath.Join(directory, "result.json")
	if err := os.Symlink(filepath.Join(directory, "missing"), path); err != nil {
		testContext.Fatal(err)
	}
	err := routingLiveWriteAtomic(path, []byte("value\n"))
	if err == nil || errors.Is(err, os.ErrNotExist) {
		testContext.Fatalf("error=%v", err)
	}
	target, err := os.Readlink(path)
	if err != nil {
		testContext.Fatal(err)
	}
	if target != filepath.Join(directory, "missing") {
		testContext.Fatalf("symlink target=%q", target)
	}
}
