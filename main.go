package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	var (
		url         string
		scenario    string
		duration    durationFlag
		minRPS      float64
		maxRPS      float64
		peakRPS     float64
		wavePeriod  durationFlag
		baseTime    durationFlag
		peakTime    durationFlag
		concurrency int
		timeout     durationFlag
		serverAddr  string
	)
	flag.StringVar(&url, "url", "http://localhost:8080", "target URL")
	flag.StringVar(&scenario, "scenario", "linear", "load scenario: linear, sine or step")
	flag.Var(&duration, "duration", "test duration, e.g. 60s, 2m")
	flag.Float64Var(&minRPS, "min-rps", 10, "minimum requests per second")
	flag.Float64Var(&maxRPS, "max-rps", 100, "maximum requests per second (linear target)")
	flag.Float64Var(&peakRPS, "peak-rps", 200, "peak requests per second (sine/step)")
	flag.Var(&wavePeriod, "period", "sine wave period, e.g. 20s")
	flag.Var(&baseTime, "base-time", "step: static load duration per cycle, e.g. 6s")
	flag.Var(&peakTime, "peak-time", "step: peak load duration per cycle, e.g. 3s")
	flag.IntVar(&concurrency, "concurrency", 50, "max concurrent HTTP clients")
	flag.Var(&timeout, "timeout", "per-request timeout, e.g. 5s")
	flag.StringVar(&serverAddr, "server", "", "run web UI server on this address, e.g. :8080")
	flag.Parse()

	if serverAddr != "" {
		if err := startServer(serverAddr); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}

	if duration <= 0 {
		fmt.Fprintln(os.Stderr, "error: -duration is required, e.g. -duration 60s")
		flag.Usage()
		os.Exit(2)
	}
	if url == "" {
		fmt.Fprintln(os.Stderr, "error: -url is required")
		os.Exit(2)
	}

	cfg := &Config{
		URL:         url,
		Scenario:    scenario,
		Duration:    time.Duration(duration),
		MinRPS:      minRPS,
		MaxRPS:      maxRPS,
		PeakRPS:     peakRPS,
		WavePeriod:  time.Duration(wavePeriod),
		BaseTime:    time.Duration(baseTime),
		PeakTime:    time.Duration(peakTime),
		Concurrency: concurrency,
		Timeout:     time.Duration(timeout),
	}

	fmt.Printf("Starting %s scenario against %s for %v\n", cfg.Scenario, cfg.URL, cfg.Duration)
	fmt.Printf("Workers: %d   MinRPS: %.1f   MaxRPS: %.1f   PeakRPS: %.1f\n",
		concurrency, cfg.MinRPS, cfg.MaxRPS, cfg.PeakRPS)

	if err := Run(context.Background(), cfg, func(e Event) {
		switch e.Type {
		case "progress":
			fmt.Printf("  elapsed %v  rps %.1f  failures %d\n",
				time.Duration(e.Elapsed*float64(time.Second)).Round(time.Second), e.RPS, e.Failures)
		case "done":
			fmt.Printf("\n=== Results ===\n")
			fmt.Printf("Duration:   %v\n", time.Duration(e.Elapsed*float64(time.Second)).Round(time.Millisecond))
			fmt.Printf("Requests:   %d\n", e.Total)
			fmt.Printf("Failures:   %d\n", e.Failures)
			if e.Total > 0 {
				fmt.Printf("RPS avg:    %.2f\n", e.RPS)
			}
			if len(e.Latency) > 0 {
				fmt.Printf("Latency     p50: %v  p90: %v  p95: %v  p99: %v\n",
					e.Latency["p50"], e.Latency["p90"], e.Latency["p95"], e.Latency["p99"])
			}
		}
	}); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
