package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
)

// Generalized parser for all read scenarios.
// Streaming sampled scenarios: check_manage_direct_user, check_manage_org_admin, check_view_via_group_member
// Enumeration scenarios: lookup_resources_manage_super, lookup_resources_view_regular
// Computes mean, p95, min, max over collected sample durations per engine + scenario.

var (
	reEngineHeader2         = regexp.MustCompile(`^==== ENGINE: (.+?) ====`)
	reStreamingStart        = regexp.MustCompile(`\[(?P<engine>[^\]]+)\] \[(?P<scenario>check_[a-z_]+)\] streaming mode\. iterations=(?P<iters>\d+)`)
	reStreamingLookupSample = regexp.MustCompile(`\[(?P<engine>[^\]]+)\] \[(?P<scenario>check_[a-z_]+)\] lookup iter=(?P<iter>\d+) .* dur=(?P<dur>[0-9.]+)(?P<unit>ms|µs|ns|s)`)
	reStreamingIterSample   = regexp.MustCompile(`\[(?P<engine>[^\]]+)\] \[(?P<scenario>check_[a-z_]+)\] iter=(?P<iter>\d+) .* dur=(?P<dur>[0-9.]+)(?P<unit>ms|µs|ns|s)`)
	reStreamingDone         = regexp.MustCompile(`\[(?P<engine>[^\]]+)\] \[(?P<scenario>check_[a-z_]+)\] DONE: iters=(?P<iters>\d+)`)
	reEnumStart             = regexp.MustCompile(`\[(?P<engine>[^\]]+)\] \[(?P<scenario>lookup_resources_[a-z_]+)\] iterations=(?P<iters>\d+) user=\d+`)
	reEnumIter              = regexp.MustCompile(`\[(?P<engine>[^\]]+)\] \[(?P<scenario>lookup_resources_[a-z_]+)\] iter=\d+ resources=(?P<count>\d+) duration=(?P<dur>[0-9.]+)(?P<unit>ms|µs|ns|s)`)
	reEnumDone              = regexp.MustCompile(`\[(?P<engine>[^\]]+)\] \[(?P<scenario>lookup_resources_[a-z_]+)\] DONE: iters=(?P<iters>\d+) lastCount=(?P<count>\d+) avg=(?P<avg>[0-9.]+)(?P<avgUnit>ms|µs|ns|s) total=(?P<total>[0-9.]+)(?P<totalUnit>ms|µs|ns|s)`)
)

type ScenarioMetrics struct {
	Engine        string
	Scenario      string
	IterationsCfg int
	Runs          int
	DurationsMs   []float64
	MeanMs        float64
	P95Ms         float64
	MinMs         float64
	MaxMs         float64
	SamplesPerRun int
	LastCount     int
}

func main() {
	logPath := "benchmark/2-3-benchmark.log"
	if len(os.Args) > 1 {
		logPath = os.Args[1]
	}
	fh, err := os.Open(logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening log: %v\n", err)
		os.Exit(1)
	}
	defer fh.Close()

	metrics := map[string]*ScenarioMetrics{}
	runStarts := map[string]int{}

	scanner := bufio.NewScanner(fh)
	for scanner.Scan() {
		line := scanner.Text()
		if reEngineHeader2.MatchString(line) {
			continue
		}
		if m := reStreamingStart.FindStringSubmatch(line); m != nil {
			engine, scenario, iters := m[1], m[2], atoi(m[3])
			key := key(engine, scenario)
			if metrics[key] == nil {
				metrics[key] = &ScenarioMetrics{Engine: engine, Scenario: scenario, IterationsCfg: iters}
			}
			runStarts[key]++
			continue
		}
		if m := reEnumStart.FindStringSubmatch(line); m != nil {
			engine, scenario, iters := m[1], m[2], atoi(m[3])
			key := key(engine, scenario)
			if metrics[key] == nil {
				metrics[key] = &ScenarioMetrics{Engine: engine, Scenario: scenario, IterationsCfg: iters}
			}
			runStarts[key]++
			continue
		}
		if m := reStreamingLookupSample.FindStringSubmatch(line); m != nil {
			engine, scenario := m[1], m[2]
			// indexes: 3=iter, 4=dur value, 5=unit
			dur := toMs(m[4], m[5])
			key := key(engine, scenario)
			if metrics[key] == nil {
				metrics[key] = &ScenarioMetrics{Engine: engine, Scenario: scenario, IterationsCfg: 1000}
			}
			metrics[key].DurationsMs = append(metrics[key].DurationsMs, dur)
			continue
		}
		if m := reStreamingIterSample.FindStringSubmatch(line); m != nil {
			engine, scenario := m[1], m[2]
			dur := toMs(m[4], m[5])
			key := key(engine, scenario)
			if metrics[key] == nil {
				metrics[key] = &ScenarioMetrics{Engine: engine, Scenario: scenario, IterationsCfg: 1000}
			}
			metrics[key].DurationsMs = append(metrics[key].DurationsMs, dur)
			continue
		}
		if m := reEnumIter.FindStringSubmatch(line); m != nil {
			engine, scenario := m[1], m[2]
			// indexes: 3=count, 4=dur value, 5=unit
			dur := toMs(m[4], m[5])
			key := key(engine, scenario)
			if metrics[key] == nil {
				metrics[key] = &ScenarioMetrics{Engine: engine, Scenario: scenario, IterationsCfg: 10}
			}
			metrics[key].DurationsMs = append(metrics[key].DurationsMs, dur)
			continue
		}
		if m := reEnumDone.FindStringSubmatch(line); m != nil {
			engine, scenario := m[1], m[2]
			// reEnumDone groups: 1=engine 2=scenario 3=iters 4=lastCount 5=avg 6=avgUnit 7=total 8=totalUnit
			lastCount := atoi(m[4])
			key := key(engine, scenario)
			if metrics[key] == nil {
				metrics[key] = &ScenarioMetrics{Engine: engine, Scenario: scenario, IterationsCfg: 10}
			}
			if lastCount > metrics[key].LastCount {
				metrics[key].LastCount = lastCount
			}
			continue
		}
		if reStreamingDone.MatchString(line) {
			continue
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan error: %v\n", err)
		os.Exit(1)
	}

	for k, sm := range metrics {
		sm.Runs = runStarts[k]
		if sm.Runs > 0 {
			sm.SamplesPerRun = len(sm.DurationsMs) / sm.Runs
		}
		if len(sm.DurationsMs) == 0 {
			continue
		}
		sorted := append([]float64{}, sm.DurationsMs...)
		sort.Float64s(sorted)
		sm.MinMs = sorted[0]
		sm.MaxMs = sorted[len(sorted)-1]
		var sum float64
		for _, v := range sm.DurationsMs {
			sum += v
		}
		sm.MeanMs = sum / float64(len(sm.DurationsMs))
		idx := int(0.95*float64(len(sorted))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		sm.P95Ms = sorted[idx]
	}

	orderEngines := []string{"authzed_crdb", "authzed_pgdb", "cockroachdb", "postgres", "scylladb", "clickhouse", "elasticsearch", "mongodb"}
	scenarios := []string{"check_manage_direct_user", "check_manage_org_admin", "check_view_via_group_member", "lookup_resources_manage_super", "lookup_resources_view_regular"}

	for _, scenario := range scenarios {
		fmt.Printf("\n## Scenario: %s\n", scenario)
		fmt.Println("| Backend | Runs | Samples/Run | Mean (ms) | p95 (ms) | Min (ms) | Max (ms) | Iterations (cfg) | LastCount/Resources | Notes |")
		fmt.Println("|---------|------|-------------|-----------|----------|----------|----------|------------------|----------------------|-------|")
		for _, engine := range orderEngines {
			key := key(engine, scenario)
			sm := metrics[key]
			if sm == nil || len(sm.DurationsMs) == 0 {
				continue
			}
			fmt.Printf("| %s | %d | %d | %s | %s | %s | %s | %d | %d | samples aggregated across runs |\n", sm.Engine, sm.Runs, sm.SamplesPerRun, fmtMs(sm.MeanMs), fmtMs(sm.P95Ms), fmtMs(sm.MinMs), fmtMs(sm.MaxMs), sm.IterationsCfg, sm.LastCount)
		}
	}
}

func key(engine, scenario string) string { return engine + "|" + scenario }

func atoi(s string) int { v, _ := strconv.Atoi(s); return v }

func toMs(valStr, unit string) float64 {
	v, _ := strconv.ParseFloat(valStr, 64)
	switch unit {
	case "ms":
		return v
	case "s":
		return v * 1000.0
	case "µs":
		return v / 1000.0
	case "ns":
		return v / 1_000_000.0
	default:
		return v
	}
}

func fmtMs(v float64) string {
	if v < 0.1 {
		return fmt.Sprintf("%.6f", v)
	}
	if v < 10 {
		return fmt.Sprintf("%.4f", v)
	}
	return fmt.Sprintf("%.2f", v)
}
