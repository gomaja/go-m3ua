module github.com/gomaja/go-m3ua

go 1.26

require (
	github.com/gomaja/go-sctp v0.0.0-20261015040308-001e3ddea46c
	github.com/google/go-cmp v0.7.0
)

require github.com/gomaja/go-ctxsync v0.0.0-20261014212015-de26fb453109 // indirect

retract [v1.0.0, v1.3.1] // superseded: go get github.com/gomaja/go-m3ua@main
