package main

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds all parameters of the load test.
type Config struct {
	URL         string
	Scenario    string
	Duration    time.Duration
	MinRPS      float64
	MaxRPS      float64
	PeakRPS     float64
	WavePeriod  time.Duration
	Concurrency int
	Timeout     time.Duration
}

// Stats aggregates request results.
type Stats struct {
	mu         sync.Mutex
	total      int64
	failures   int64
	latencies  []time.Duration
	start      time.Time
	lastSample time.Time
}

func (s *Stats) add(latency time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.total++
	if err != nil {
		s.failures++
	}
	s.latencies = append(s.latencies, latency)
	s.lastSample = time.Now()
}

func (s *Stats) percentiles() map[string]time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.latencies
	if len(l) == 0 {
		return nil
	}
	sorted := make([]time.Duration, len(l))
	copy(sorted, l)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	res := map[string]time.Duration{}
	for _, p := range []float64{50, 90, 95, 99} {
		idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		res[fmt.Sprintf("p%v", p)] = sorted[idx]
	}
	return res
}

func (s *Stats) report(done time.Duration) {
	s.mu.Lock()
	total, failures := s.total, s.failures
	lat := s.latencies
	s.mu.Unlock()
	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Duration:   %v\n", done.Round(time.Millisecond))
	fmt.Printf("Requests:   %d\n", total)
	fmt.Printf("Failures:   %d\n", failures)
	if total > 0 {
		fmt.Printf("RPS avg:    %.2f\n", float64(total)/done.Seconds())
	}
	if len(lat) > 0 {
		percs := s.percentiles()
		fmt.Printf("Latency     p50: %v  p90: %v  p95: %v  p99: %v\n",
			percs["p50"], percs["p90"], percs["p95"], percs["p99"])
	}
}

// scheduler computes the target RPS for a given elapsed time.
func scheduler(cfg *Config) func(elapsed time.Duration) float64 {
	if cfg.Scenario == "sine" {
		period := cfg.WavePeriod
		if period <= 0 {
			period = cfg.Duration / 4
		}
		return func(elapsed time.Duration) float64 {
			phase := 2 * math.Pi * elapsed.Seconds() / period.Seconds()
			amp := (cfg.PeakRPS - cfg.MinRPS) / 2
			mid := (cfg.PeakRPS + cfg.MinRPS) / 2
			return mid + amp*math.Sin(phase)
		}
	}
	// linear growth from MinRPS to MaxRPS over the whole duration.
	return func(elapsed time.Duration) float64 {
		frac := math.Min(1.0, elapsed.Seconds()/cfg.Duration.Seconds())
		return cfg.MinRPS + (cfg.MaxRPS-cfg.MinRPS)*frac
	}
}

// Run executes the load test according to cfg.
func Run(cfg *Config) error {
	client := &http.Client{Timeout: cfg.Timeout}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workers := cfg.Concurrency
	if workers <= 0 {
		workers = 1
	}

	target := scheduler(cfg)
	stats := &Stats{start: time.Now()}

	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)

	start := time.Now()
	period := 100 * time.Millisecond
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	fmt.Printf("Starting %s scenario against %s for %v\n", cfg.Scenario, cfg.URL, cfg.Duration)
	fmt.Printf("Workers: %d   MinRPS: %.1f   MaxRPS: %.1f   PeakRPS: %.1f\n",
		workers, cfg.MinRPS, cfg.MaxRPS, cfg.PeakRPS)

	// progress printer
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				el := time.Since(start)
				rps := float64(atomic.LoadInt64(&stats.total)) / el.Seconds()
				fmt.Printf("  elapsed %v  rps(avg) %.1f  failures %d\n",
					el.Round(time.Second), rps, stats.failures)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			stats.report(time.Since(start))
			return nil
		case <-ticker.C:
			elapsed := time.Since(start)
			if elapsed >= cfg.Duration {
				cancel()
				continue
			}
			rps := target(elapsed)
			// number of requests to fire in this tick window.
			n := int(rps * period.Seconds())
			for i := 0; i < n; i++ {
				select {
				case sem <- struct{}{}:
					wg.Add(1)
					go func() {
						defer wg.Done()
						defer func() { <-sem }()
						reqStart := time.Now()
						resp, err := client.Get(cfg.URL)
						var lat time.Duration
						if err != nil {
							lat = time.Since(reqStart)
						} else {
							lat = time.Since(reqStart)
							_ = resp.Body.Close()
						}
						stats.add(lat, err)
					}()
				default:
					// concurrency limit reached; skip request.
				}
			}
		}
	}
}
