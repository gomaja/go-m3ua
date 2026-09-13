package main

import (
	"runtime"
	"runtime/debug"
)

func currentManifest(outstandingLimit int) fixtureManifest {
	manifest := fixtureManifest{
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
