// Package simulator provides the demo's fake buyers and traffic generator.
// Buyer behaviour follows a time-phased scenario, so a full story (outage,
// block, recovery, relapse) plays out without manual steps, and can be
// overridden at runtime over HTTP.
package simulator

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Scenario describes the fake buyers and the traffic sent to them.
type Scenario struct {
	// Latency is a healthy response's base latency.
	Latency Duration `yaml:"latency"`
	// TimeoutLatency is how long a "timeout" response takes. It should exceed
	// the auction pods' deadline.
	TimeoutLatency Duration `yaml:"timeout_latency"`
	// RPS is the number of ad requests per second sent to the auction pods.
	RPS int `yaml:"rps"`
	// Buyers are the fake buyers.
	Buyers []BuyerSpec `yaml:"buyers"`
}

// BuyerSpec is one fake buyer.
type BuyerSpec struct {
	ID string `yaml:"id"`
	// Share is the fraction of ad requests this buyer is eligible for, which
	// sets its traffic volume. Default 1.
	Share *float64 `yaml:"share"`
	// Phases run in order; the last one lasts forever.
	Phases []Phase `yaml:"phases"`
}

// Phase is a period of fixed behaviour.
type Phase struct {
	// Duration is ignored on the last phase.
	Duration Duration `yaml:"duration"`
	Behavior `yaml:",inline"`
}

// Behavior is how a buyer responds.
type Behavior struct {
	FailureRate float64 `yaml:"failure_rate" json:"failure_rate"`
	TimeoutRate float64 `yaml:"timeout_rate" json:"timeout_rate"`
}

func (b Behavior) validate() error {
	if b.FailureRate < 0 || b.TimeoutRate < 0 || b.FailureRate+b.TimeoutRate > 1 {
		return fmt.Errorf("failure_rate and timeout_rate must be >= 0 and sum to <= 1, got %v and %v", b.FailureRate, b.TimeoutRate)
	}
	return nil
}

// ShareOrDefault returns the buyer's share of ad requests.
func (b BuyerSpec) ShareOrDefault() float64 {
	if b.Share == nil {
		return 1
	}
	return *b.Share
}

// At returns the buyer's behaviour at elapsed time since the scenario began.
func (b BuyerSpec) At(elapsed time.Duration) Behavior {
	for i, p := range b.Phases {
		if i == len(b.Phases)-1 || elapsed < p.Duration.D() {
			return p.Behavior
		}
		elapsed -= p.Duration.D()
	}
	return Behavior{}
}

// Duration is a time.Duration that unmarshals from strings like "30s".
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	v, err := time.ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*d = Duration(v)
	return nil
}

// LoadScenario reads and validates a YAML scenario file.
func LoadScenario(path string) (Scenario, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Scenario{}, err
	}
	return ParseScenario(raw)
}

// ParseScenario parses and validates a YAML scenario.
func ParseScenario(raw []byte) (Scenario, error) {
	var s Scenario
	if err := yaml.Unmarshal(raw, &s); err != nil {
		return Scenario{}, fmt.Errorf("parse scenario: %w", err)
	}
	if s.Latency == 0 {
		s.Latency = Duration(10 * time.Millisecond)
	}
	if s.TimeoutLatency == 0 {
		s.TimeoutLatency = Duration(300 * time.Millisecond)
	}
	return s, s.validate()
}

func (s Scenario) validate() error {
	var errs []error
	if s.RPS <= 0 {
		errs = append(errs, errors.New("rps must be > 0"))
	}
	if len(s.Buyers) == 0 {
		errs = append(errs, errors.New("at least one buyer is required"))
	}
	seen := map[string]bool{}
	for _, b := range s.Buyers {
		switch {
		case b.ID == "":
			errs = append(errs, errors.New("buyer id is required"))
		case seen[b.ID]:
			errs = append(errs, fmt.Errorf("duplicate buyer %q", b.ID))
		}
		seen[b.ID] = true
		if sh := b.ShareOrDefault(); sh <= 0 || sh > 1 {
			errs = append(errs, fmt.Errorf("buyer %q: share must be in (0, 1], got %v", b.ID, sh))
		}
		if len(b.Phases) == 0 {
			errs = append(errs, fmt.Errorf("buyer %q: at least one phase is required", b.ID))
		}
		for i, p := range b.Phases {
			if err := p.validate(); err != nil {
				errs = append(errs, fmt.Errorf("buyer %q phase %d: %w", b.ID, i, err))
			}
			if i < len(b.Phases)-1 && p.Duration <= 0 {
				errs = append(errs, fmt.Errorf("buyer %q phase %d: duration is required except on the last phase", b.ID, i))
			}
		}
	}
	return errors.Join(errs...)
}
