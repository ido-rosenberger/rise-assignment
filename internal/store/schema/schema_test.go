package schema

import (
	"testing"
	"time"

	"github.com/ido-rosenberger/rise-assignment/buyer"
)

func TestStatsKey(t *testing.T) {
	w := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if got, want := StatsKey(w), "stats:1767268800"; got != want {
		t.Fatalf("StatsKey = %q, want %q", got, want)
	}
	// Same instant in another time zone maps to the same key.
	if got := StatsKey(w.In(time.FixedZone("X", 3*3600))); got != "stats:1767268800" {
		t.Fatalf("StatsKey depends on time zone: %q", got)
	}
}

func TestParseStats(t *testing.T) {
	got, invalid := ParseStats(map[string]string{
		StatsField("b1", FieldTotal):       "10",
		StatsField("b1", FieldFailed):      "2",
		StatsField("b1", FieldTimeouts):    "1",
		StatsField("ns:b2", FieldTotal):    "5", // buyer ID containing ':'
		StatsField("ns:b2", FieldTimeouts): "5",
	})
	if len(invalid) != 0 {
		t.Fatal(invalid)
	}
	want := map[string]buyer.Stats{
		"b1":    {Total: 10, Failed: 2, Timeouts: 1},
		"ns:b2": {Total: 5, Timeouts: 5},
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("%s = %+v, want %+v", id, got[id], w)
		}
	}
}

// Each bad field is reported and skipped; good fields in the same hash are
// still returned.
func TestParseStatsSkipsBadFields(t *testing.T) {
	cases := []struct {
		name  string
		field string
		value string
	}{
		{"no separator", "b1total", "1"},
		{"empty buyer ID", ":total", "1"},
		{"unknown counter", "b1:errors", "1"},
		{"not a number", "b1:failed", "x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, invalid := ParseStats(map[string]string{
				tc.field:                     tc.value,
				StatsField("b2", FieldTotal): "7",
			})
			if len(invalid) != 1 {
				t.Fatalf("invalid = %v, want 1 error", invalid)
			}
			if got["b2"].Total != 7 {
				t.Fatalf("good field lost: %v", got)
			}
		})
	}
}
