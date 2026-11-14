package congestionmethod

import "testing"

func FuzzCongestionMethodParsing(f *testing.F) {
	for _, value := range []string{"", "international", "national-with-priorities", "national-without-priorities", "automatic", "0", "\xff"} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value string) {
		method, err := Parse(value)
		want := uint8(0)
		switch value {
		case "international":
			want = 1
		case "national-with-priorities":
			want = 2
		case "national-without-priorities":
			want = 3
		}
		if want == 0 {
			if err == nil || method != 0 {
				t.Fatalf("invalid method accepted: %q => %d, %v", value, method, err)
			}
		} else if err != nil || uint8(method) != want {
			t.Fatalf("method choice changed: %q => %d, %v", value, method, err)
		}
	})
}
