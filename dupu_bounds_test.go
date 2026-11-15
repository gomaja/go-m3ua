package m3ua

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestDUPUStructuralBoundsAcrossAdmissionPaths(t *testing.T) {
	// These are structural envelope tests, not assertions that reserved values
	// have assignments in any MTP3 variant (RFC 4666 §3.4.5; Q.Imp704 §2.2.4).
	for _, pair := range [][2]uint16{{0, 0}, {8, 1}, {11, 2}, {6, 3}, {255, 15}, {256, 0}, {3, 16}, {65535, 65535}} {
		user, cause := pair[0], pair[1]
		valid := user <= 255 && cause <= 15
		for _, path := range []string{"typed", "raw", "inbound"} {
			t.Run(fmt.Sprintf("%s/user%d/cause%d", path, user, cause), func(t *testing.T) {
				var err error
				msg := messages.NewDestinationUserPartUnavailable(nil, nil, apc(0x1234), params.NewUserCause(user, cause), nil)
				switch path {
				case "typed":
					e, newErr := NewEndpoint(nationalCongestionEndpointConfig(EndpointConfig{Role: RoleSGP}))
					if newErr != nil {
						t.Fatal(newErr)
					}
					t.Cleanup(func() { _ = e.Close() })
					err = e.DestinationUserPartUnavailable(DestinationUserPartUnavailableRequest{Destination: PointCodeRange{PointCode: 0x1234}, User: user, Cause: cause})
				case "raw":
					c, sent := newSSNMTestConn(t, StateASPActive, RoleSGP)
					_, err = c.WriteSignal(msg)
					if (len(*sent) == 1) != valid {
						t.Errorf("writes = %d, valid %v", len(*sent), valid)
					}
				case "inbound":
					c, _ := newSSNMTestConn(t, StateASPActive, RoleASP)
					raw, marshalErr := msg.MarshalBinary()
					if marshalErr != nil {
						t.Fatal(marshalErr)
					}
					parsed, parseErr := messages.Parse(raw)
					if parseErr != nil {
						t.Fatal(parseErr)
					}
					err = c.handleDestinationUserPartUnavailable(parsed.(*messages.DestinationUserPartUnavailable))
					if err == nil {
						report := nextSSNMReport(t, c)
						if report.UserCause != uint32(cause)<<16|uint32(user) {
							t.Fatalf("User/Cause changed: %#x", report.UserCause)
						}
					}
				}
				if valid && err != nil {
					t.Fatalf("structural envelope rejected: %v", err)
				}
				if !valid && !errors.Is(err, ErrInvalidParameterValue) {
					t.Fatalf("outside structural envelope error = %v, want ErrInvalidParameterValue", err)
				}
			})
		}
	}
}
