//go:build !linux

package main

import "fmt"

const interfaceRoot = ""

func platformSocketOptions(int) (socketOptions, error) {
	return socketOptions{}, fmt.Errorf("socket options: %w", errUnavailable)
}

func ethtoolOffloads(string) (map[string]bool, map[string]string) {
	return nil, map[string]string{"offloads": errUnavailable.Error()}
}
