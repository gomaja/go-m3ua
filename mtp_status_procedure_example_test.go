package m3ua_test

import (
	"fmt"

	"github.com/gomaja/go-m3ua"
)

func ExampleMTPStatusProcedure() {
	procedure, err := m3ua.NewMTPStatusProcedure(m3ua.CongestionProfile{Method: m3ua.CongestionNationalWithoutPriorities})
	if err != nil {
		panic(err)
	}
	status := m3ua.MTPDestinationStatus{Destination: m3ua.MTPDestination{MTPRoute: "local-sccp", PointCode: 123}, Availability: m3ua.DestinationAvailable, Congested: true, CongestionLevel: 2, CongestionLevelSet: true}
	// One application adapter fans out every returned primitive to each user.
	// Call Offered once per original request before submitting through raw APIs.
	users := [2][]int{}
	for offer := 1; offer <= 17; offer++ {
		indication, err := procedure.Offered(status)
		if err != nil {
			panic(err)
		}
		if indication != nil {
			for user := range users {
				users[user] = append(users[user], offer)
			}
		}
	}
	fmt.Println("user 0:", users[0])
	fmt.Println("user 1:", users[1])
	// Output:
	// user 0: [1 9 17]
	// user 1: [1 9 17]
}
