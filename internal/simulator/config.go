// Package simulator provides the demo's fake buyers (an HTTP server whose
// failure and timeout rates follow a schedule and can be changed live) and a
// traffic generator that sends auctions to the auction pods.
package simulator

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

// Config is the simulator.json file.
type Config struct {
	// RPS is the total auctions per second sent across all pods.
	RPS int `json:"rps"`
	// Pods are auction pod base URLs, used round-robin.
	Pods   []string      `json:"pods"`
	Buyers []BuyerConfig `json:"buyers"`
}

// BuyerConfig describes one fake buyer.
type BuyerConfig struct {
	ID string `json:"id"`
	// Participation is the probability the buyer is invited to an auction.
	Participation float64 `json:"participation"`
	Rates
	// Incident optionally overrides Rates for a period after start.
	Incident *Incident `json:"incident,omitempty"`
}

// Rates are the probabilities that one bid request fails or times out.
type Rates struct {
	FailureRate float64 `json:"failure_rate"`
	TimeoutRate float64 `json:"timeout_rate"`
}

func (r Rates) validate() error {
	if r.FailureRate < 0 || r.TimeoutRate < 0 || r.FailureRate+r.TimeoutRate > 1 {
		return fmt.Errorf("rates must be >= 0 and sum to <= 1, got failure %v timeout %v", r.FailureRate, r.TimeoutRate)
	}
	return nil
}

// Incident applies Rates from Start to End, measured from simulator start.
type Incident struct {
	Start Duration `json:"start"`
	End   Duration `json:"end"`
	Rates
}

// Duration is a time.Duration written as a string in JSON ("30s").
type Duration time.Duration

// UnmarshalJSON parses a duration string such as "30s".
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"30s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// MarshalJSON writes the duration as a string such as "30s".
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// LoadConfig reads and validates a config file.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := c.Validate(); err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

// Validate reports every invalid field.
func (c Config) Validate() error {
	var errs []error
	if c.RPS <= 0 {
		errs = append(errs, fmt.Errorf("rps must be > 0, got %d", c.RPS))
	}
	if len(c.Pods) == 0 {
		errs = append(errs, errors.New("pods must not be empty"))
	}
	if len(c.Buyers) == 0 {
		errs = append(errs, errors.New("buyers must not be empty"))
	}
	seen := map[string]bool{}
	for _, b := range c.Buyers {
		if b.ID == "" {
			errs = append(errs, errors.New("buyer id must not be empty"))
		}
		if seen[b.ID] {
			errs = append(errs, fmt.Errorf("duplicate buyer id %q", b.ID))
		}
		seen[b.ID] = true
		if b.Participation <= 0 || b.Participation > 1 {
			errs = append(errs, fmt.Errorf("buyer %q: participation must be in (0, 1], got %v", b.ID, b.Participation))
		}
		if err := b.Rates.validate(); err != nil {
			errs = append(errs, fmt.Errorf("buyer %q: %w", b.ID, err))
		}
		if in := b.Incident; in != nil {
			if in.Start < 0 || in.End <= in.Start {
				errs = append(errs, fmt.Errorf("buyer %q: incident needs 0 <= start < end", b.ID))
			}
			if err := in.Rates.validate(); err != nil {
				errs = append(errs, fmt.Errorf("buyer %q incident: %w", b.ID, err))
			}
		}
	}
	return errors.Join(errs...)
}
