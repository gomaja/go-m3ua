// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"testing"
)

// Exercise both real queue operations, including the public dequeue path.
// The queue never overflows, so this measures ordinary receive cost.
func BenchmarkReceiveDataQueue(benchmark *testing.B) {
	for _, capacity := range []int{1, 2} {
		benchmark.Run(fmt.Sprint(capacity), func(benchmark *testing.B) {
			c := newReceiveRoutingContextAssociation(benchmark, 1, false)
			c.dataChan = make(chan *DataMessage, capacity)
			data := inboundData(1, string(make([]byte, 128)))
			ctx := context.Background()
			benchmark.ReportAllocs()
			for benchmark.Loop() {
				c.handleData(ctx, data, nil)
				if _, err := c.ReadData(ctx); err != nil {
					benchmark.Fatal(err)
				}
			}
		})
	}
}
