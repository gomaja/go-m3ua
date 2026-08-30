package params

import "testing"

func TestOriginatingPointCodeListEntriesRoundTrip(t *testing.T) {
	want := []PointCodeWithMask{
		{Mask: 0, PointCode: 0x00112233},
		{Mask: 8, PointCode: 0xff445566},
	}
	parameter := NewOriginatingPointCodeListWithMasks(want...)
	got := parameter.OriginatingPointCodeListEntries()
	if len(got) != len(want) {
		t.Fatalf("entries = %v, want %v", got, want)
	}
	for index := range want {
		if got[index].Mask != want[index].Mask || got[index].PointCode != want[index].PointCode&0x00ffffff {
			t.Errorf("entry %d = %+v, want Mask %d PointCode %#x", index, got[index], want[index].Mask, want[index].PointCode&0x00ffffff)
		}
	}
}

func TestOriginatingPointCodeListEntriesRejectWrongOrMalformedParameter(t *testing.T) {
	if got := NewRoutingContext(1).OriginatingPointCodeListEntries(); got != nil {
		t.Fatalf("wrong-tag entries = %v, want nil", got)
	}
	malformed := &Param{Tag: OriginatingPointCodeList, Data: []byte{1, 2, 3}}
	if got := malformed.OriginatingPointCodeListEntries(); got != nil {
		t.Fatalf("malformed entries = %v, want nil", got)
	}
}
