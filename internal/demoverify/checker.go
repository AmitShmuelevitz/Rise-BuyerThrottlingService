// Package demoverify turns the docker-compose demo into an acceptance test:
// it watches the live throttle state and checks it against the expectations
// written next to each buyer in the scenario file.
package demoverify

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/simulator"
)

// Observation is one buyer's state as served by GET /state.
type Observation struct {
	Rate   float64
	Reason string
}

type buyerTrack struct {
	expect  []simulator.Expectation
	last    *Observation
	blocked bool                           // has been at the block level
	met     map[simulator.Expectation]bool // positive expectations seen so far
	broken  []string                       // violations of never_throttled
	history []float64                      // distinct rates, for failure messages
}

// Checker accumulates observations and decides when every expectation holds.
type Checker struct {
	blockLevel float64
	buyers     map[string]*buyerTrack
}

// NewChecker builds a checker for the scenario's buyers that have expectations.
func NewChecker(buyers []simulator.BuyerSpec, blockLevel float64) *Checker {
	c := &Checker{blockLevel: blockLevel, buyers: make(map[string]*buyerTrack)}
	for _, b := range buyers {
		if len(b.Expect) == 0 {
			continue
		}
		c.buyers[b.ID] = &buyerTrack{expect: b.Expect, met: make(map[simulator.Expectation]bool)}
	}
	return c
}

// Observe records the current state of every buyer. Buyers missing from the
// state have not been seen yet and are treated as open.
func (c *Checker) Observe(state map[string]Observation) {
	for id, t := range c.buyers {
		o := state[id]
		if t.last != nil && *t.last == o {
			continue
		}
		c.transition(id, t, o)
		t.last = &o
		t.history = append(t.history, o.Rate)
	}
}

func (c *Checker) transition(id string, t *buyerTrack, o Observation) {
	atBlock := o.Rate >= c.blockLevel
	if o.Rate > 0 && slices.Contains(t.expect, simulator.ExpectNeverThrottled) {
		t.broken = append(t.broken, fmt.Sprintf("%s was throttled to %v (%s)", id, o.Rate, o.Reason))
	}
	if atBlock {
		switch o.Reason {
		case "failure_rate":
			t.met[simulator.ExpectBlockedOnFailures] = true
		case "timeout_rate":
			t.met[simulator.ExpectBlockedOnTimeouts] = true
		}
		if t.last != nil && t.last.Rate > 0 && t.last.Rate < c.blockLevel {
			t.met[simulator.ExpectSnapBack] = true
		}
		t.blocked = true
	}
	if o.Rate == 0 && t.blocked {
		t.met[simulator.ExpectRecovered] = true
	}
}

// Failed returns violations that can no longer be undone.
func (c *Checker) Failed() []string {
	var out []string
	for _, t := range c.buyers {
		out = append(out, t.broken...)
	}
	sort.Strings(out)
	return out
}

// Pending returns the positive expectations not met yet, as "buyer: expectation".
func (c *Checker) Pending() []string {
	var out []string
	for id, t := range c.buyers {
		for _, e := range t.expect {
			if e != simulator.ExpectNeverThrottled && !t.met[e] {
				out = append(out, fmt.Sprintf("%s: %s (rates seen: %s)", id, e, rates(t.history)))
			}
		}
	}
	sort.Strings(out)
	return out
}

// Done reports whether every positive expectation was met and nothing failed.
func (c *Checker) Done() bool { return len(c.Failed()) == 0 && len(c.Pending()) == 0 }

func rates(h []float64) string {
	parts := make([]string, len(h))
	for i, r := range h {
		parts[i] = fmt.Sprint(r)
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " -> ")
}
