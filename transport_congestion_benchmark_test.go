package m3ua

import (
	"fmt"
	"testing"

	"github.com/gomaja/go-sctp"
)

// These measurements include commit/publication checks with an injected cheap
// reader. They exclude the kernel query and timer waits. Ten sweeps per second
// is the default schedule, so ns/op * 10 estimates CPU ns/s at the named size.
func BenchmarkTransportCongestionSampler(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			e, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = e.Close() })
			backlog := &sctp.SendBacklog{Limit: 65536}
			e.transport.read = func(*Association) (*sctp.SendBacklog, error) { return backlog, nil }
			jobs := make([]transportCongestionJob, count)
			for i := range jobs {
				a := newAssociation(RoleSGP, NewAssociationConfig())
				a.state = StateASPActive
				if !e.trackAssociation(a) {
					b.Fatal("track")
				}
				jobs[i] = transportCongestionJob{a, a.ID(), a.Epoch()}
				e.transport.sample(e, jobs[i])
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for _, job := range jobs {
					e.transport.sample(e, job)
				}
			}
		})
	}
}
