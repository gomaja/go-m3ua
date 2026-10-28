module github.com/gomaja/go-m3ua

go 1.26

require (
	github.com/gomaja/go-ctxsync v0.0.0-20261015170731-23c9cb67b19b
	github.com/gomaja/go-pointcode v0.0.0-20261015102047-53f963a604bd
	github.com/gomaja/go-sctp v0.0.0-20261028085618-54173fb6ecaa
	github.com/google/go-cmp v0.7.0
)

retract [v1.0.0, v1.3.1] // superseded: go get github.com/gomaja/go-m3ua@main
