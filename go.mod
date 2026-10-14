module github.com/gomaja/go-m3ua

go 1.26

require (
	github.com/gomaja/go-sctp v0.0.0-20261014114506-6dc7c7827367
	github.com/google/go-cmp v0.7.0
	github.com/pascaldekloe/goe v0.1.1
)

retract [v1.0.0, v1.3.1] // superseded: go get github.com/gomaja/go-m3ua@main
