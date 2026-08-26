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
	BaseTime    time.Duration
	PeakTime    time.Duration
	Concurrency int
	Timeout     time.Duration
}

// Event is streamed to subscribers during a load test run.
type Event struct {
	Type     string            `json:"type"` // "start" | "progress" | "done"
	Elapsed  float64           `json:"elapsed"`
	Total    int64             `json:"total"`
	Failures int64             `json:"failures"`
	RPS      float64           `json:"rps"`
	Latency  map[string]string `json:"latency,omitempty"`
}

// Stats aggregates request results.
type Stats struct {
	mu        sync.Mutex
	total     atomic.Int64
	failures  atomic.Int64
	latencies []time.Duration
}

func (s *Stats) add(latency time.Duration, err error) {
	s.total.Add(1)
	if err != nil {
		s.failures.Add(1)
	}
	s.mu.Lock()
	s.latencies = append(s.latencies, latency)
	s.mu.Unlock()
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
	if cfg.Scenario == "step" {
		base := cfg.BaseTime
		peak := cfg.PeakTime
		if base <= 0 {
			base = cfg.WavePeriod / 2
		}
		if peak <= 0 {
			peak = cfg.WavePeriod / 2
		}
		cycle := base + peak
		return func(elapsed time.Duration) float64 {
			// static MinRPS for BaseTime, then PeakRPS for PeakTime, repeating.
			if elapsed%cycle < base {
				return cfg.MinRPS
			}
			return cfg.PeakRPS
		}
	}
	// linear growth from MinRPS to MaxRPS over the whole duration.
	return func(elapsed time.Duration) float64 {
		frac := math.Min(1.0, elapsed.Seconds()/cfg.Duration.Seconds())
		return cfg.MinRPS + (cfg.MaxRPS-cfg.MinRPS)*frac
	}
}

// Run executes the load test according to cfg, streaming events to onEvent.
// onEvent may be nil. The test stops when ctx is canceled or duration elapses.
func Run(ctx context.Context, cfg *Config, onEvent func(Event)) error {
	client := &http.Client{Timeout: cfg.Timeout}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := cfg.Concurrency
	if workers <= 0 {
		workers = 1
	}

	target := scheduler(cfg)
	stats := &Stats{}

	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)

	emit := func(e Event) {
		if onEvent != nil {
			onEvent(e)
		}
	}

	emit(Event{
		Type:     "start",
		Elapsed:  0,
		Total:    0,
		Failures: 0,
		RPS:      target(0),
	})

	start := time.Now()
	period := 100 * time.Millisecond
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	// progress emitter
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		lastTotal := int64(0)
		lastTime := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				now := time.Now()
				total := stats.total.Load()
				instRPS := float64(total-lastTotal) / now.Sub(lastTime).Seconds()
				lastTotal = total
				lastTime = now
				emit(Event{
					Type:     "progress",
					Elapsed:  now.Sub(start).Seconds(),
					Total:    total,
					Failures: stats.failures.Load(),
					RPS:      instRPS,
				})
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			<-done
			wg.Wait()
			el := time.Since(start)
			lat := stats.percentiles()
			latStr := make(map[string]string, len(lat))
			for k, v := range lat {
				latStr[k] = v.String()
			}
			emit(Event{
				Type:     "done",
				Elapsed:  el.Seconds(),
				Total:    stats.total.Load(),
				Failures: stats.failures.Load(),
				RPS:      float64(stats.total.Load()) / el.Seconds(),
				Latency:  latStr,
			})
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
						lat := time.Since(reqStart)
						if err == nil {
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
