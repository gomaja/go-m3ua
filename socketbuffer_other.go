// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

//go:build !linux

package m3ua

import "github.com/gomaja/go-sctp"

// setSocketBuffers is not reached where go-sctp has no SCTP: Dial and Listen
// fail with sctp.ErrUnsupported before any Control hook runs. It reports the
// same error rather than claiming the sizes were applied.
func setSocketBuffers(uintptr, socketBuffers) error {
	return sctp.ErrUnsupported
}
