package main

import (
	bufio "bufio"
	fmt "fmt"
	os "os"
	regexp "regexp"
	sort "sort"
	strconv "strconv"
)

var (
	reEngineHeader1 = regexp.MustCompile(`^==== ENGINE: (.+?) ====`)
	reScenarioStart = regexp.MustCompile(`\[(?P<engine>[^\]]+)\] \[check_manage_direct_user\] streaming mode\. iterations=(?P<iters>\d+)`)
	reLookup        = regexp.MustCompile(`\[(?P<engine>[^\]]+)\] \[check_manage_direct_user\] lookup iter=(?P<iter>\d+) resource=\d+ user=\d+ dur=(?P<dur>[0-9.]+)(?P<unit>ms|µs)`)
	reHeavyDone     = regexp.MustCompile(`\[(?P<engine>[^\]]+)\] \[lookup_resources_manage_super\] DONE: iters=\d+ lastCount=(?P<count>\d+)`)
)

type EngineMetrics struct {
	Engine           string    `json:"engine"`
	IterationsCfg    int       `json:"iterations_cfg"`
	Runs             int       `json:"runs"`
	SamplesPerRun    int       `json:"samples_per_run"`
	DurationsMs      []float64 `json:"durations_ms"`
	MeanMs           float64   `json:"mean_ms"`
	P95Ms            float64   `json:"p95_ms"`
	MinMs            float64   `json:"min_ms"`
	MaxMs            float64   `json:"max_ms"`
	SampleLimit      int       `json:"sample_limit"`
	ResourcesChecked int       `json:"resources_checked"`
	HeavyResources   int       `json:"heavy_resources"`
}

func main() {
	logPath := "benchmark/2-3-benchmark.log"
	if len(os.Args) > 1 {
		logPath = os.Args[1]
	}
	fh, err := os.Open(logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer fh.Close()

	engines := map[string]*EngineMetrics{}
	scanner := bufio.NewScanner(fh)

	// Track seen run starts for direct user per engine to count runs
	seenRunStarts := map[string]int{}
	// Heavy resource counts (lastCount) per engine
	heavyCounts := map[string]int{}

	for scanner.Scan() {
		line := scanner.Text()
		if m := reEngineHeader1.FindStringSubmatch(line); m != nil {
			// engine header encountered; nothing to do beyond recognizing boundary
			continue
		}
		if m := reScenarioStart.FindStringSubmatch(line); m != nil {
			engine := m[1]
			itersStr := m[2]
			iters, _ := strconv.Atoi(itersStr)
			if engines[engine] == nil {
				engines[engine] = &EngineMetrics{Engine: engine, IterationsCfg: iters, SampleLimit: 100}
			}
			seenRunStarts[engine]++
			continue
		}
		if m := reLookup.FindStringSubmatch(line); m != nil {
			engine := m[1]
			d := m[3]
			unit := m[4]
			val, _ := strconv.ParseFloat(d, 64)
			if unit == "µs" { // convert microseconds to ms
				val = val / 1000.0
			}
			if engines[engine] == nil {
				engines[engine] = &EngineMetrics{Engine: engine, IterationsCfg: 1000, SampleLimit: 100}
			}
			engines[engine].DurationsMs = append(engines[engine].DurationsMs, val)
			continue
		}
		if m := reHeavyDone.FindStringSubmatch(line); m != nil {
			engine := m[1]
			countStr := m[2]
			count, _ := strconv.Atoi(countStr)
			heavyCounts[engine] = count
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan error: %v\n", err)
		os.Exit(1)
	}

	// Compute metrics
	for _, em := range engines {
		em.Runs = seenRunStarts[em.Engine]
		if em.Runs > 0 {
			// samples per run: occurrences of iter=0,100,...,900
			em.SamplesPerRun = len(em.DurationsMs) / em.Runs
		}
		if len(em.DurationsMs) > 0 {
			var sum float64
			min := em.DurationsMs[0]
			max := em.DurationsMs[0]
			for _, v := range em.DurationsMs {
				sum += v
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
			em.MeanMs = sum / float64(len(em.DurationsMs))
			// p95
			sorted := append([]float64{}, em.DurationsMs...)
			sort.Float64s(sorted)
			idx := int(0.95*float64(len(sorted))) - 1
			if idx < 0 {
				idx = 0
			}
			if idx >= len(sorted) {
				idx = len(sorted) - 1
			}
			em.P95Ms = sorted[idx]
			em.MinMs = min
			em.MaxMs = max
			// Resources checked: assume IterationsCfg * Runs (approx) when scenario executed each run
			em.ResourcesChecked = em.IterationsCfg * em.Runs
			// Heavy resources from lookup_resources_manage_super if captured
			em.HeavyResources = heavyCounts[em.Engine]
		}
	}

	// Output markdown rows for table 6.2.1
	fmt.Println("# Debug: durations counts per engine")
	for k, em := range engines {
		fmt.Printf("engine=%s runs=%d samples=%d firstSamples=%v\n", k, em.Runs, len(em.DurationsMs), head(em.DurationsMs, 5))
	}
	fmt.Println("# Table 6.2.1 Generated Rows")
	fmt.Println("| Backend | Iterations (cfg) | Completed | Mean (ms) | p95 (ms) | Min (ms) | Max (ms) | Sample Limit | Resources Checked | Resources/User (heavy) | Notes |")
	fmt.Println("|---------|------------------|-----------|-----------|----------|----------|----------|--------------|-------------------|------------------------|-------|")
	order := []string{"authzed_crdb", "authzed_pgdb", "cockroachdb", "postgres", "scylladb", "clickhouse", "elasticsearch", "mongodb"}
	for _, name := range order {
		em := engines[name]
		if em == nil {
			continue
		}
		fmt.Printf("| %s | %d | %d | %.2f | %.2f | %.2f | %.2f | %d | %d | %d | Samples every 100; %d runs |\n",
			em.Engine, em.IterationsCfg, em.ResourcesChecked, em.MeanMs, em.P95Ms, em.MinMs, em.MaxMs, em.SampleLimit, em.ResourcesChecked, em.HeavyResources, em.Runs)
	}
}

func head(a []float64, n int) []float64 {
	if len(a) < n {
		return a
	}
	return a[:n]
}
