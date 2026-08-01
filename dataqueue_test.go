// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestNewConnResolvesDataQueueSize(t *testing.T) {
	for _, test := range []struct {
		name       string
		configured int
		want       int
	}{
		{name: "unset", configured: 0, want: DefaultDataQueueSize},
		{name: "negative", configured: -1, want: DefaultDataQueueSize},
		{name: "one", configured: 1, want: 1},
		{name: "custom", configured: 2048, want: 2048},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := NewConfig(1, 2, params.ServiceIndSCCP, 0, 0, 1)
			cfg.DataQueueSize = test.configured

			conn := newConn(modeClient, cfg)
			if got := cap(conn.dataChan); got != test.want {
				t.Errorf("data queue capacity = %d, want %d", got, test.want)
			}
			if cfg.DataQueueSize != test.configured {
				t.Errorf("newConn changed Config.DataQueueSize from %d to %d",
					test.configured, cfg.DataQueueSize)
			}
		})
	}
}

func TestDefaultDataQueueSize(t *testing.T) {
	if DefaultDataQueueSize != 1024 {
		t.Errorf("DefaultDataQueueSize = %d, want 1024", DefaultDataQueueSize)
	}
}
