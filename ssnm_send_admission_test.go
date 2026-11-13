package m3ua

import (
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestDUPUSendRequiresSingleUnmaskedDestination(t *testing.T) {
	for _, test := range []struct {
		name  string
		apc   *params.Param
		valid bool
	}{
		{"single", params.NewAffectedPointCode(100), true},
		{"list", params.NewAffectedPointCode(100, 200), false},
		{"range", params.NewAffectedPointCodeWithMask(1, 100), false},
		{"empty", params.NewAffectedPointCode(), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, builder := range []string{"typed", "generic"} {
				t.Run(builder, func(t *testing.T) {
					for _, path := range []string{"public", "control", "distributed"} {
						c, sent := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
						rc, apc, cause := params.NewRoutingContext(1), test.apc.Copy(), params.NewUserCause(params.SCCP, params.Unequipped)
						var msg messages.M3UA = messages.NewDestinationUserPartUnavailable(nil, rc, apc, cause, nil)
						if builder == "generic" {
							msg = messages.New(1, messages.MsgClassSSNM, messages.MsgTypeDestinationUserPartUnavailable, rc, apc, cause)
						}
						// Codecs retain captures with lists and masks. Empty APC was already
						// refused by parameter serialization before send admission.
						raw, codecErr := msg.MarshalBinary()
						if test.name != "empty" {
							if codecErr != nil {
								t.Fatal(codecErr)
							}
							decoded, err := messages.Parse(raw)
							if err != nil {
								t.Fatal(err)
							}
							roundTrip, err := decoded.MarshalBinary()
							if err != nil {
								t.Fatal(err)
							}
							if string(roundTrip) != string(raw) {
								t.Fatal("capture did not round-trip")
							}
						} else if !errors.Is(codecErr, params.ErrInvalidLength) {
							t.Fatalf("empty codec: %v", codecErr)
						}
						var err error
						switch path {
						case "public":
							_, err = c.WriteSignal(msg)
						case "control":
							_, err = c.writeControl(msg)
						case "distributed":
							_, err = c.writeDistributedSignal(msg)
						}
						switch {
						case test.valid:
							if err != nil || len(*sent) != 1 {
								t.Fatalf("valid DUPU: %v sent=%d", err, len(*sent))
							}
						case test.name == "empty":
							if !errors.Is(err, params.ErrInvalidLength) || len(*sent) != 0 {
								t.Fatalf("empty admission: %v", err)
							}
						default:
							if !errors.Is(err, ErrInvalidParameterValue) || len(*sent) != 0 {
								t.Fatalf("path=%s err=%v sent=%d", path, err, len(*sent))
							}
						}
					}
				})
			}
		})
	}
}
