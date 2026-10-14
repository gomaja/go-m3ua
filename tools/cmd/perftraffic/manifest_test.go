package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestManifestMatchesAssociationSocketSettings(testContext *testing.T) {
	manifest := currentManifest(maxOutstanding, initiationASPDial)
	if manifest.Initiation != initiationASPDial {
		testContext.Fatalf("manifest initiation = %q, want %q", manifest.Initiation, initiationASPDial)
	}
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

// Issue #36 requires the campaign to name the baseline commit it is assessed
// against. The manifest's own vcs_revision stamps the fixture binary's head,
// which is the candidate, so the baseline needs a field of its own and a label
// that cannot be mistaken for it.
func TestManifestRecordsTheAssessedBaseline(testContext *testing.T) {
	encoded, err := json.Marshal(currentManifest(maxOutstanding, initiationASPDial))
	if err != nil {
		testContext.Fatalf("marshal manifest: %v", err)
	}
	const want = `"assessed_baseline_revision":"d097e191d879efc95e36c0254814933f01aa9aee"`
	if !strings.Contains(string(encoded), want) {
		testContext.Fatalf("manifest does not record the assessed baseline %s: %s", want, encoded)
	}
	manifest := currentManifest(maxOutstanding, initiationASPDial)
	if manifest.AssessedBaselineRevision != assessedBaselineRevision {
		testContext.Fatalf("manifest baseline = %q, want %q", manifest.AssessedBaselineRevision, assessedBaselineRevision)
	}
	if manifest.AssessedBaselineRevision == manifest.VCSRevision {
		testContext.Fatalf("the assessed baseline and the fixture head are the same commit %q; they identify different things", manifest.VCSRevision)
	}
	for _, initiation := range []string{initiationASPDial, initiationSGPDial} {
		if currentManifest(maxOutstanding, initiation).AssessedBaselineRevision != assessedBaselineRevision {
			testContext.Fatalf("%s run reports a different assessed baseline", initiation)
		}
	}
}

// The baseline is a full commit identifier, not an abbreviation that could
// become ambiguous as the history grows.
func TestAssessedBaselineIsAFullCommitIdentifier(testContext *testing.T) {
	if len(assessedBaselineRevision) != 40 {
		testContext.Fatalf("assessed baseline %q is %d characters, want a full 40-character commit identifier", assessedBaselineRevision, len(assessedBaselineRevision))
	}
	for index, character := range assessedBaselineRevision {
		if !strings.ContainsRune("0123456789abcdef", character) {
			testContext.Fatalf("assessed baseline %q has a non-hexadecimal character %q at %d", assessedBaselineRevision, character, index)
		}
	}
}
