// Package config reads service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/amit/buyer-throttling/internal/policy"
)

// Env reads typed values from environment variables and collects every parse
// error, so a misconfigured service reports all problems at once.
type Env struct {
	lookup func(string) (string, bool)
	errs   []error
}

// FromOS reads the process environment.
func FromOS() *Env { return &Env{lookup: os.LookupEnv} }

// FromMap reads a fixed map (for tests).
func FromMap(m map[string]string) *Env {
	return &Env{lookup: func(k string) (string, bool) { v, ok := m[k]; return v, ok }}
}

// Err returns all errors seen so far.
func (e *Env) Err() error { return errors.Join(e.errs...) }

func (e *Env) String(key, def string) string {
	if v, ok := e.lookup(key); ok && v != "" {
		return v
	}
	return def
}

func (e *Env) Duration(key string, def time.Duration) time.Duration {
	v, ok := e.lookup(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return d
}

func (e *Env) Float(key string, def float64) float64 {
	v, ok := e.lookup(key)
	if !ok || v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return f
}

func (e *Env) Int(key string, def int64) int64 {
	v, ok := e.lookup(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}

// Policy reads the throttling policy (THROTTLE_*) and validates it.
func (e *Env) Policy() policy.Config {
	d := policy.DefaultConfig()
	cfg := policy.Config{
		FailureThreshold:     e.Float("THROTTLE_FAILURE_THRESHOLD", d.FailureThreshold),
		TimeoutThreshold:     e.Float("THROTTLE_TIMEOUT_THRESHOLD", d.TimeoutThreshold),
		MinSampleSize:        e.Int("THROTTLE_MIN_SAMPLE_SIZE", d.MinSampleSize),
		BlockLevel:           e.Float("THROTTLE_BLOCK_LEVEL", d.BlockLevel),
		RecoveryStep:         e.Float("THROTTLE_RECOVERY_STEP", d.RecoveryStep),
		Window:               e.Duration("THROTTLE_WINDOW", d.Window),
		StaleRecoveryWindows: int(e.Int("THROTTLE_STALE_RECOVERY_WINDOWS", int64(d.StaleRecoveryWindows))),
	}
	if err := cfg.Validate(); err != nil {
		e.errs = append(e.errs, err)
	}
	return cfg
}

// Logger builds a JSON logger at LOG_LEVEL (debug, info, warn, error).
func (e *Env) Logger(service string) *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(e.String("LOG_LEVEL", "info")))); err != nil {
		e.errs = append(e.errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})).With("service", service)
}
