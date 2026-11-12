// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"testing"
)

func TestSCTPQueueReadsWithoutAssociation(t *testing.T) {
	c := &Association{}
	if got, err := c.SCTPSendBacklog(); got != nil || !errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("SCTPSendBacklog = %+v, %v; want nil, ErrAssociationClosed", got, err)
	}
	if got, err := c.SCTPAssocQueues(); got != nil || !errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("SCTPAssocQueues = %+v, %v; want nil, ErrAssociationClosed", got, err)
	}
}
