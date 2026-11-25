package config

import (
	"fmt"
	"os"
	"time"
)

// String returns the env var key, or def when unset.
func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

// Duration returns the env var key parsed as a duration ("5s", "1m"), or def
// when unset.
func Duration(key string, def time.Duration) (time.Duration, error) {
	return parseEnv(key, def, time.ParseDuration)
}

// parseEnv is the one place env vars are parsed: def when unset, def and an
// error naming the variable when invalid.
func parseEnv[T any](key string, def T, parse func(string) (T, error)) (T, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	v, err := parse(raw)
	if err != nil {
		return def, fmt.Errorf("%s=%q: %w", key, raw, err)
	}
	return v, nil
}
