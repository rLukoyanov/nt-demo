package main

import (
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
		concurrency int
		timeout     durationFlag
	)
	flag.StringVar(&url, "url", "http://localhost:8080", "target URL")
	flag.StringVar(&scenario, "scenario", "linear", "load scenario: linear or sine")
	flag.Var(&duration, "duration", "test duration, e.g. 60s, 2m")
	flag.Float64Var(&minRPS, "min-rps", 10, "minimum requests per second")
	flag.Float64Var(&maxRPS, "max-rps", 100, "maximum requests per second (linear target)")
	flag.Float64Var(&peakRPS, "peak-rps", 200, "peak requests per second (sine)")
	flag.Var(&wavePeriod, "period", "sine wave period, e.g. 20s")
	flag.IntVar(&concurrency, "concurrency", 50, "max concurrent HTTP clients")
	flag.Var(&timeout, "timeout", "per-request timeout, e.g. 5s")
	flag.Parse()

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
		Concurrency: concurrency,
		Timeout:     time.Duration(timeout),
	}

	if err := Run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
