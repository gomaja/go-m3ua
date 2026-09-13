package main

import "testing"

func TestManifestMatchesAssociationSocketSettings(testContext *testing.T) {
	manifest := currentManifest(maxOutstanding)
	for _, role := range []string{"asp", "sgp"} {
		config := associationConfig(role)
		if !config.SCTPNoDelayInfo.Enabled || !config.SCTPSACKInfo.Enabled ||
			manifest.SCTPNoDelay != config.SCTPNoDelayInfo.NoDelay ||
			manifest.SCTPSACKDelay != config.SCTPSACKInfo.SackDelay ||
			manifest.SCTPSACKFrequency != config.SCTPSACKInfo.SackFrequency {
			testContext.Fatalf("%s socket settings differ from manifest: %+v", role, manifest)
		}
	}
}
