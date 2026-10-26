package metrics

import (
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"
)

// TestAlertRulesUseExportedMetrics guards deploy/alerts.yml against metric
// renames: every metric an alert reads must be one this package exports, or
// the alert silently never fires. promtool checks the rules' logic
// (make test-alerts); this checks they match the code.
func TestAlertRulesUseExportedMetrics(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/alerts.yml")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Groups []struct {
			Rules []struct {
				Alert  string            `yaml:"alert"`
				Expr   string            `yaml:"expr"`
				Labels map[string]string `yaml:"labels"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	if err := yaml.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}

	reg := prometheus.NewRegistry()
	th := NewThrottler(reg, time.Minute)
	th.AddCalls("b", 1, 0, 0) // vectors appear in Gather only once they have a series
	th.AddSkipped("b", 1)
	th.SetRates(map[string]float64{"b": 0})
	ev := NewEvaluator(reg)
	ev.ObserveRun("error", 0)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	exported := make(map[string]bool)
	for _, mf := range mfs {
		exported[mf.GetName()] = true
	}

	metricName := regexp.MustCompile(`\b(?:throttle|evaluator|buyer)_[a-z_]+\b`)
	rules := 0
	for _, g := range file.Groups {
		for _, r := range g.Rules {
			rules++
			if r.Labels["severity"] == "" {
				t.Errorf("%s: no severity label", r.Alert)
			}
			names := metricName.FindAllString(r.Expr, -1)
			if len(names) == 0 {
				t.Errorf("%s: expression %q reads no service metric", r.Alert, r.Expr)
			}
			for _, n := range names {
				if !exported[n] {
					t.Errorf("%s: metric %s is not exported", r.Alert, n)
				}
			}
		}
	}
	if rules == 0 {
		t.Fatal("no alert rules found")
	}
}
