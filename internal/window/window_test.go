package window

import (
	"testing"
	"time"
)

func TestOf(t *testing.T) {
	size := time.Minute
	base := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		t    time.Time
		want time.Time
	}{
		{"start of window", base, base},
		{"middle of window", base.Add(30 * time.Second), base},
		{"last nanosecond of window", base.Add(time.Minute - time.Nanosecond), base},
		{"next window", base.Add(time.Minute), base.Add(time.Minute)},
		{"non-UTC zone aligns the same", base.Add(10 * time.Second).In(time.FixedZone("x", 3*3600+1800)), base},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Of(tt.t, size).Start(); !got.Equal(tt.want) {
				t.Errorf("Of(%v).Start() = %v, want %v", tt.t, got, tt.want)
			}
		})
	}
}

func TestOf_BeforeEpoch(t *testing.T) {
	got := Of(time.Unix(-5, 0), 10*time.Second)
	if got != ID(-10) {
		t.Fatalf("Of(-5s) = %d, want -10", got)
	}
}

func TestNavigation(t *testing.T) {
	size := 10 * time.Second
	id := Of(time.Unix(1_000_005, 0), size)
	if id != 1_000_000 {
		t.Fatalf("id = %d", id)
	}
	if id.Next(size) != 1_000_010 || id.Prev(size) != 999_990 {
		t.Fatalf("next/prev = %d/%d", id.Next(size), id.Prev(size))
	}
	if !id.End(size).Equal(id.Next(size).Start()) {
		t.Fatal("end of a window must be the start of the next")
	}
	if id.String() != "1000000" {
		t.Fatalf("String() = %q", id.String())
	}
}
