package main

import (
	bufio "bufio"
	flag "flag"
	fmt "fmt"
	os "os"
	regexp "regexp"
	strconv "strconv"
	strings "strings"
)

type agg struct {
	count  int
	cpuSum float64
	cpuMin float64
	cpuMax float64
	memSum float64
	memMin float64
	memMax float64
}

func update(a *agg, cpu, mem float64) {
	if a.count == 0 {
		a.cpuMin, a.cpuMax = cpu, cpu
		a.memMin, a.memMax = mem, mem
	} else {
		if cpu < a.cpuMin {
			a.cpuMin = cpu
		}
		if cpu > a.cpuMax {
			a.cpuMax = cpu
		}
		if mem < a.memMin {
			a.memMin = mem
		}
		if mem > a.memMax {
			a.memMax = mem
		}
	}
	a.count++
	a.cpuSum += cpu
	a.memSum += mem
}

func mean(total float64, count int) float64 {
	if count == 0 {
		return 0
	}
	return total / float64(count)
}

func main() {
	cpus := flag.Float64("cpus", 0, "Allocated Docker CPUs (normalize CPU%) — if set, 100% equals full allocation usage (raw/CPUs)")
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s [--cpus N] <monitor.log>\n", os.Args[0])
		os.Exit(1)
	}
	logPath := flag.Arg(0)
	f, err := os.Open(logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open log failed: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	engineAgg := make(map[string]*agg)
	currentEngine := ""

	engineRe := regexp.MustCompile(`^\s*==== ENGINE: ([a-zA-Z0-9_\-]+) ====\s*$`)
	lineRe := regexp.MustCompile(`^\d{4}/\d{2}/\d{2} .* \[(.+?)\] CPU: ([0-9.]+)% \| Memory: ([0-9.]+)% \(.+\)$`)

	for scanner.Scan() {
		line := scanner.Text()
		if m := engineRe.FindStringSubmatch(line); m != nil {
			currentEngine = m[1]
			// ensure map entry exists
			if _, ok := engineAgg[currentEngine]; !ok {
				engineAgg[currentEngine] = &agg{}
			}
			continue
		}
		m := lineRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		// only aggregate when we have a current engine context
		if currentEngine == "" {
			continue
		}
		cpuStr := strings.TrimSuffix(m[2], "%")
		memStr := strings.TrimSuffix(m[3], "%")
		cpu, err1 := strconv.ParseFloat(cpuStr, 64)
		mem, err2 := strconv.ParseFloat(memStr, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		// Normalize CPU%: Docker stats reports percent across all cores (e.g., 800% for 8 cores fully used).
		// When --cpus is provided, divide by that count to make 100% represent full allocation.
		if *cpus > 0 {
			cpu = cpu / *cpus
		}
		a := engineAgg[currentEngine]
		if a == nil {
			na := &agg{}
			engineAgg[currentEngine] = na
			a = na
		}
		update(a, cpu, mem)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan log failed: %v\n", err)
		os.Exit(1)
	}

	// Output markdown table (no Notes column)
	fmt.Println("| Backend | CPU Mean (%) | CPU Min (%) | CPU Max (%) | RAM Mean (%) | RAM Min (%) | RAM Max (%) | Samples |")
	fmt.Println("|---------|---------------|-------------|-------------|--------------|-------------|-------------|---------|")
	// deterministic order: list common engines
	order := []string{"authzed_crdb", "authzed_pgdb", "cockroachdb", "postgres", "scylladb", "clickhouse", "elasticsearch", "mongodb"}
	seen := make(map[string]bool)
	for _, eng := range order {
		if a, ok := engineAgg[eng]; ok && a.count > 0 {
			fmt.Printf("| %s | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %d |\n",
				eng, mean(a.cpuSum, a.count), a.cpuMin, a.cpuMax, mean(a.memSum, a.count), a.memMin, a.memMax, a.count)
			seen[eng] = true
		}
	}
	// any remaining engines
	for eng, a := range engineAgg {
		if seen[eng] || a.count == 0 {
			continue
		}
		fmt.Printf("| %s | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %d |\n",
			eng, mean(a.cpuSum, a.count), a.cpuMin, a.cpuMax, mean(a.memSum, a.count), a.memMin, a.memMax, a.count)
	}
}
