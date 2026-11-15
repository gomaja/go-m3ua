package main

import (
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua"
)

func TestCommandRequiresCongestionMethod(t *testing.T) {
	if _, err := parseConfig([]string{"-role=asp", "-peer-control=http://peer:8080"}); err == nil {
		t.Fatal("missing congestion method accepted")
	}
}

func TestCommandSelectsExplicitCongestionMethod(t *testing.T) {
	for _, tc := range []struct {
		name   string
		method m3ua.CongestionMethod
	}{{"international", m3ua.CongestionInternational}, {"national-with-priorities", m3ua.CongestionNationalWithPriorities}, {"national-without-priorities", m3ua.CongestionNationalWithoutPriorities}} {
		t.Run(tc.name, func(t *testing.T) {
			config, err := parseConfig([]string{"-role=asp", "-peer-control=http://peer:8080", "-congestion-method=" + tc.name})
			if err != nil {
				t.Fatal(err)
			}
			field := reflect.ValueOf(config).FieldByName("CongestionMethod")
			if !field.IsValid() || field.Uint() != uint64(tc.method) {
				t.Fatalf("method not retained: %+v", config)
			}
		})
	}
	for _, method := range []string{"", "automatic", "0"} {
		if _, err := parseConfig([]string{"-role=asp", "-peer-control=http://peer:8080", "-congestion-method=" + method}); err == nil {
			t.Fatalf("invalid method %q accepted", method)
		}
	}
}
