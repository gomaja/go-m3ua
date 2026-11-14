package main

import (
	"context"
	"testing"

	"github.com/gomaja/go-m3ua"
)

func nationalCongestionEndpointConfig(config m3ua.EndpointConfig) m3ua.EndpointConfig {
	if config.Congestion == nil || config.Congestion.Default.Method == 0 {
		config.Congestion = &m3ua.CongestionConfig{Default: m3ua.CongestionProfile{Method: m3ua.CongestionNationalWithPriorities, MaxLevel: 3}}
	}
	return config
}

func nationalCongestionArguments(arguments []string) []string {
	for _, argument := range arguments {
		if len(argument) >= len("-congestion-method") && argument[:len("-congestion-method")] == "-congestion-method" {
			return arguments
		}
	}
	return append([]string{"-congestion-method=national-with-priorities"}, arguments...)
}

func nationalCongestionCommandConfig(config commandConfig) commandConfig {
	if config.CongestionMethod == 0 {
		config.CongestionMethod = m3ua.CongestionNationalWithPriorities
	}
	return config
}

// runSenderWithoutTransportCongestion isolates accounting/drain tests from the
// independent backlog-derived local-user gate, using the public configuration.
func runSenderWithoutTransportCongestion(t *testing.T, ctx context.Context, config commandConfig) (combinedResult, error) {
	t.Helper()
	endpointConfig := senderEndpointConfig(config)
	endpointConfig.TransportCongestion = &m3ua.TransportCongestionConfig{Disabled: true}
	endpoint, err := m3ua.NewEndpoint(endpointConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = endpoint.Close() }()
	return runDirectSender(ctx, config, endpoint)
}
