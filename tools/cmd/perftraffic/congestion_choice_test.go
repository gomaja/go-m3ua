package main

import (
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua"
)

func TestCommandRequiresCongestionMethod(t *testing.T) {
	if _, err := parseConfig([]string{"-role=sgp", "-transport=listen"}); err == nil {
		t.Fatal("missing congestion method accepted")
	}
}

func TestCommandCongestionMethodReachesEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name   string
		method m3ua.CongestionMethod
	}{{"national-with-priorities", m3ua.CongestionNationalWithPriorities}} {
		t.Run(tc.name, func(t *testing.T) {
			config, err := parseConfig([]string{"-role=sgp", "-transport=listen", "-congestion-method=" + tc.name})
			if err != nil {
				t.Fatal(err)
			}
			field := reflect.ValueOf(config).FieldByName("CongestionMethod")
			if !field.IsValid() || field.Uint() != uint64(tc.method) {
				t.Fatalf("method not retained: %+v", config)
			}
			endpoint := senderEndpointConfig(config)
			if endpoint.Congestion == nil || endpoint.Congestion.Default.Method != tc.method {
				t.Fatalf("method lost at Endpoint: %+v", endpoint)
			}
		})
	}
	for _, method := range []string{"", "automatic", "0"} {
		if _, err := parseConfig([]string{"-congestion-method=" + method}); err == nil {
			t.Fatalf("invalid method %q accepted", method)
		}
	}
}

func TestTrafficFixtureRejectsMethodsWithoutMessagePriorities(t *testing.T) {
	for _, method := range []string{"international", "national-without-priorities"} {
		if _, err := parseConfig([]string{"-role=sgp", "-transport=listen", "-congestion-method=" + method}); err == nil {
			t.Errorf("MP0..3 traffic accepted incompatible method %q", method)
		}
	}
}
