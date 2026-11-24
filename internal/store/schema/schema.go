// Package schema is the Redis layout shared by the pod adapter
// (throttler/redisstore) and the evaluator adapter (internal/store): key
// names, stats field names and JSON value encodings. Both sides import it, so
// the layout is defined once.
package schema

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ido-rosenberger/rise-assignment/buyer"
)

// Keys. Stats keys are per window; the others are single keys.
const (
	// KeyState: hash buyer -> StateValue JSON. Read by pods.
	KeyState = "throttle:state"
	// KeyEval: hash buyer -> EvalValue JSON. Evaluator only.
	KeyEval = "throttle:eval"
	// KeyVersion: integer, incremented on every state write.
	KeyVersion = "throttle:version"
	// KeyLastWindow: unix seconds of the last evaluated window start.
	KeyLastWindow = "throttle:last_window"

	statsKeyPrefix = "stats:"
)

// StatsRetentionWindows is how many evaluation windows a stats key lives.
// It bounds how far back the evaluator can catch up after downtime.
const StatsRetentionWindows = 10

// Stats hash field suffixes: field = buyerID + ":" + suffix.
const (
	FieldTotal    = "total"
	FieldFailed   = "failed"
	FieldTimeouts = "timeouts"
)

// StatsKey is the hash holding every buyer's stats for one window.
func StatsKey(windowStart time.Time) string {
	return statsKeyPrefix + strconv.FormatInt(windowStart.Unix(), 10)
}

// StatsField is the field for one buyer counter in a stats hash.
func StatsField(buyerID, suffix string) string {
	return buyerID + ":" + suffix
}

// ParseStats decodes a stats hash (as returned by HGETALL) into per-buyer
// stats. Buyer IDs may contain ':'; the suffix is after the last one.
// Malformed fields are skipped and returned as errors, so one bad field does
// not hide every other buyer's stats.
func ParseStats(fields map[string]string) (stats map[string]buyer.Stats, invalid []error) {
	stats = make(map[string]buyer.Stats)
	for field, raw := range fields {
		i := strings.LastIndexByte(field, ':')
		if i <= 0 {
			invalid = append(invalid, fmt.Errorf("stats field %q: missing buyer ID", field))
			continue
		}
		buyerID, suffix := field[:i], field[i+1:]
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			invalid = append(invalid, fmt.Errorf("stats field %q: %w", field, err))
			continue
		}
		s := stats[buyerID]
		switch suffix {
		case FieldTotal:
			s.Total = n
		case FieldFailed:
			s.Failed = n
		case FieldTimeouts:
			s.Timeouts = n
		default:
			invalid = append(invalid, fmt.Errorf("stats field %q: unknown counter %q", field, suffix))
			continue
		}
		stats[buyerID] = s
	}
	return stats, invalid
}

// StateValue is the JSON stored per buyer in KeyState.
type StateValue struct {
	ThrottleRate float64 `json:"throttle_rate"`
	Reason       string  `json:"reason"`
}

// EvalValue is the JSON stored per buyer in KeyEval.
type EvalValue struct {
	Accumulated        StatsValue `json:"accumulated"`
	AccumulatedWindows int        `json:"accumulated_windows"`
}

// StatsValue is buyer.Stats with JSON names.
type StatsValue struct {
	Total    int64 `json:"total"`
	Failed   int64 `json:"failed"`
	Timeouts int64 `json:"timeouts"`
}

func ToStatsValue(s buyer.Stats) StatsValue {
	return StatsValue{Total: s.Total, Failed: s.Failed, Timeouts: s.Timeouts}
}

func (v StatsValue) Stats() buyer.Stats {
	return buyer.Stats{Total: v.Total, Failed: v.Failed, Timeouts: v.Timeouts}
}
