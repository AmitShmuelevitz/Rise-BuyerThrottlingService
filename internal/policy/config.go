// Package policy holds the throttling rules: given a buyer's previous state and
// the stats of one closed window, it decides the buyer's next state. It is pure
// (no I/O, no clock) so every rule can be tested in isolation.
package policy

import (
	"errors"
	"fmt"
	"time"
)

// Config holds the tunables of the throttling policy.
type Config struct {
	// FailureThreshold blocks a buyer when failed/total reaches it.
	FailureThreshold float64
	// TimeoutThreshold blocks a buyer when timeouts/total reaches it.
	TimeoutThreshold float64
	// MinSampleSize is the minimum number of requests a window needs before
	// its rates are trusted.
	MinSampleSize int64
	// BlockLevel is the throttle rate applied when a buyer is blocked. It must
	// stay below 1 so a blocked buyer keeps receiving some traffic.
	BlockLevel float64
	// RecoveryStep is how much the throttle rate drops after each clean window.
	RecoveryStep float64
	// Window is the length of one evaluation window.
	Window time.Duration
	// StaleRecoveryWindows, when > 0, lets a throttled buyer take one recovery
	// step after this many consecutive under-sample windows. 0 disables it, so
	// under-sample windows always hold the current rate.
	StaleRecoveryWindows int
}

// DefaultConfig returns the defaults from the product definition.
func DefaultConfig() Config {
	return Config{
		FailureThreshold:     0.7,
		TimeoutThreshold:     0.3,
		MinSampleSize:        100,
		BlockLevel:           0.95,
		RecoveryStep:         0.1,
		Window:               time.Minute,
		StaleRecoveryWindows: 0,
	}
}

// Validate reports every invalid field at once.
func (c Config) Validate() error {
	var errs []error
	if c.FailureThreshold <= 0 || c.FailureThreshold > 1 {
		errs = append(errs, fmt.Errorf("failure threshold must be in (0, 1], got %v", c.FailureThreshold))
	}
	if c.TimeoutThreshold <= 0 || c.TimeoutThreshold > 1 {
		errs = append(errs, fmt.Errorf("timeout threshold must be in (0, 1], got %v", c.TimeoutThreshold))
	}
	if c.MinSampleSize < 1 {
		errs = append(errs, fmt.Errorf("min sample size must be >= 1, got %d", c.MinSampleSize))
	}
	if c.BlockLevel <= 0 || c.BlockLevel >= 1 {
		errs = append(errs, fmt.Errorf("block level must be in (0, 1), got %v", c.BlockLevel))
	}
	if c.RecoveryStep <= 0 || c.RecoveryStep > 1 {
		errs = append(errs, fmt.Errorf("recovery step must be in (0, 1], got %v", c.RecoveryStep))
	}
	if c.Window < time.Second || c.Window%time.Second != 0 {
		errs = append(errs, fmt.Errorf("window must be a whole number of seconds >= 1s, got %v", c.Window))
	}
	if c.StaleRecoveryWindows < 0 {
		errs = append(errs, fmt.Errorf("stale recovery windows must be >= 0, got %d", c.StaleRecoveryWindows))
	}
	return errors.Join(errs...)
}
