package m3ua

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func denseAuditFixture(tb testing.TB, count, scopes int) (*Association, *messages.DestinationStateAudit) {
	tb.Helper()
	cfg := NewAssociationConfig()
	contexts := make([]uint32, scopes)
	for i := range contexts {
		contexts[i] = uint32(i + 1)
	}
	setInventoryRoutingContexts(&cfg.ApplicationServers, params.NewRoutingContext(contexts...))
	setInventoryNetworkAppearance(&cfg.ApplicationServers, params.NewNetworkAppearance(7))
	c := newAssociation(RoleSGP, cfg)
	tb.Cleanup(func() { _ = c.Close() })
	records := make([]destinationRange, count)
	for i := range records {
		records[i] = destinationRange{NetworkAppearance: 7, NetworkAppearanceSet: true, PointCode: uint32(i), State: availabilityState(DestinationAvailable)}
	}
	c.destinations.setRanges(records)
	return c, messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(contexts...), params.NewAffectedPointCodeWithMask(14, 0), nil)
}

func TestDAUDDensePlanningDoesNotRescanEveryInterval(t *testing.T) {
	if os.Getenv("M3UA_DENSE_AUDIT_CHILD") == "1" {
		c, d := denseAuditFixture(t, 16384, 100)
		replies, err := c.destinationAuditReplies(d, []uint32{0}, []uint8{14})
		if err != nil || len(replies) != 1 {
			t.Fatalf("dense plan entries=%d err=%v", len(replies), err)
		}
		return
	}
	// This is deliberately generous for an in-memory sweep. The old nested
	// scans perform 26,843,545,600 record visits and cannot finish this input.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// os.Args[0] can be a bare file name (WSL runs Windows test binaries that
	// way), which exec then looks up in PATH; os.Executable is the binary itself.
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locate the test binary: %v", err)
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestDAUDDensePlanningDoesNotRescanEveryInterval$", "-test.count=1")
	cmd.Env = append(os.Environ(), "M3UA_DENSE_AUDIT_CHILD=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("dense audit planning failed within generous deadline: %v: %s", err, output)
	}
}

func BenchmarkDAUDDenseDestinations(b *testing.B) {
	for _, scopes := range []int{1, 100} {
		name := "1-scope"
		if scopes == 100 {
			name = "100-scopes"
		}
		b.Run(name, func(b *testing.B) {
			c, d := denseAuditFixture(b, 16384, scopes)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := c.destinationAuditReplies(d, []uint32{0}, []uint8{14}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestDAUDPlanningBoundsAllocationsAcrossScopes(t *testing.T) {
	c, d := denseAuditFixture(t, 16384, 100)
	allocations := testing.AllocsPerRun(1, func() {
		replies, err := c.destinationAuditReplies(d, []uint32{0}, []uint8{14})
		if err != nil || len(replies) != 1 {
			t.Fatalf("dense plan entries=%d err=%v", len(replies), err)
		}
	})
	// Allow one allocation per retained record, far above the scope index
	// and result allocations. Record visits in each scope must not require
	// their own heap allocation for this compact, identical-scope plan.
	if allocations > 16384 {
		t.Fatalf("dense 100-scope plan allocated %.0f times, want at most 16384", allocations)
	}
}
