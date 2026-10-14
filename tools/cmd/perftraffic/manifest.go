package main

import (
	"runtime"
	"runtime/debug"
)

// assessedBaselineRevision is the baseline commit the performance campaign is
// assessed against, as issue #36 requires. It is fixed here rather than read
// from the build: the fixture binary is built from the candidate head, so the
// build stamp records the candidate and can never name the baseline. The two
// revisions are reported as separate, separately labelled manifest fields for
// that reason.
const assessedBaselineRevision = "d097e191d879efc95e36c0254814933f01aa9aee"

func currentManifest(outstandingLimit int, initiation string) fixtureManifest {
	manifest := fixtureManifest{
		AssessedBaselineRevision: assessedBaselineRevision,

		GoVersion:         runtime.Version(),
		GoOS:              runtime.GOOS,
		GoArch:            runtime.GOARCH,
		SCTPModule:        "github.com/gomaja/go-sctp",
		GOMAXPROCS:        runtime.GOMAXPROCS(0),
		SCTPNoDelay:       sctpNoDelay,
		SCTPSACKDelay:     sctpSACKDelay,
		SCTPSACKFrequency: sctpSACKFrequency,
		FlowCount:         flowCount,
		OutstandingLimit:  outstandingLimit,
		Initiation:        initiation,
		AccountingScope:   wholeProcessScope,
	}
	if build, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range build.Settings {
			switch setting.Key {
			case "vcs.revision":
				manifest.VCSRevision = setting.Value
			case "vcs.modified":
				manifest.VCSModified = setting.Value == "true"
			}
		}
		for _, dependency := range build.Deps {
			if dependency.Path == manifest.SCTPModule {
				manifest.SCTPVersion = dependency.Version
			}
		}
	}
	return manifest
}
