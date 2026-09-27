module github.com/gomaja/go-m3ua

go 1.26

require (
	github.com/gomaja/go-sctp v1.1.1-0.20260927070356-42731fdc8a3a
	github.com/google/go-cmp v0.7.0
	github.com/pascaldekloe/goe v0.1.1
)

retract [v1.0.0, v1.2.0] // superseded: go get github.com/gomaja/go-m3ua@main
