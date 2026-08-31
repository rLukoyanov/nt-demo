package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// workerState holds the latest event received from a single worker pod.
type workerState struct {
	mu     sync.Mutex
	latest Event
	done   bool
}

func (w *workerState) update(e Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.latest = e
	if e.Type == "done" {
		w.done = true
	}
}

func (w *workerState) fail() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.done = true
}

// divideConfig returns a copy of cfg with RPS scaled down for one of n workers.
func divideConfig(cfg *Config, n int) *Config {
	if n <= 0 {
		n = 1
	}
	c := *cfg
	c.MinRPS = cfg.MinRPS / float64(n)
	c.MaxRPS = cfg.MaxRPS / float64(n)
	c.PeakRPS = cfg.PeakRPS / float64(n)
	return &c
}

func requestFromConfig(cfg *Config) runRequest {
	return runRequest{
		URL:         cfg.URL,
		Scenario:    cfg.Scenario,
		Duration:    cfg.Duration.String(),
		MinRPS:      cfg.MinRPS,
		MaxRPS:      cfg.MaxRPS,
		PeakRPS:     cfg.PeakRPS,
		WavePeriod:  cfg.WavePeriod.String(),
		BaseTime:    cfg.BaseTime.String(),
		PeakTime:    cfg.PeakTime.String(),
		Concurrency: cfg.Concurrency,
		Timeout:     cfg.Timeout.String(),
	}
}

// handleDistributedRun starts the same test on every pod in req.Pods and
// aggregates their events into a single stream for the UI.
func (s *server) handleDistributedRun(w http.ResponseWriter, req runRequest) {
	cfg, err := cfgFromRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	id := fmt.Sprintf("%d", runCounter.Add(1))
	ctx, cancel := context.WithCancel(context.Background())
	rs := &runState{id: id, cancel: cancel, subs: map[chan Event]struct{}{}}

	s.mu.Lock()
	s.runs[id] = rs
	s.mu.Unlock()

	go func() {
		defer func() {
			rs.cancel()
			s.mu.Lock()
			delete(s.runs, id)
			s.mu.Unlock()
			rs.close()
		}()
		runDistributed(ctx, rs, req.Pods, cfg)
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"id": id})
}

func runDistributed(ctx context.Context, rs *runState, pods []string, cfg *Config) {
	n := len(pods)
	if n == 0 {
		return
	}
	workers := make([]*workerState, n)
	for i := range workers {
		workers[i] = &workerState{}
	}

	var wg sync.WaitGroup
	for i, pod := range pods {
		wg.Add(1)
		go func(i int, pod string) {
			defer wg.Done()
			runWorker(ctx, workers[i], pod, divideConfig(cfg, n))
		}(i, pod)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		aggregate(ctx, rs, workers)
	}()
	wg.Wait()
}

// runWorker starts a run on one pod and consumes its SSE stream until the
// stream closes or ctx is canceled.
func runWorker(ctx context.Context, ws *workerState, pod string, cfg *Config) {
	body, err := json.Marshal(requestFromConfig(cfg))
	if err != nil {
		ws.fail()
		return
	}

	startCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(startCtx, http.MethodPost, pod+"/api/run", bytes.NewReader(body))
	if err != nil {
		ws.fail()
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		ws.fail()
		return
	}
	var out struct {
		ID string `json:"id"`
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if err != nil || out.ID == "" {
		ws.fail()
		return
	}

	streamURL := pod + "/api/stream?id=" + out.ID
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	if err != nil {
		ws.fail()
		return
	}
	sresp, err := http.DefaultClient.Do(req)
	if err != nil {
		ws.fail()
		return
	}
	defer sresp.Body.Close()

	scanner := bufio.NewScanner(sresp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var e Event
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &e) != nil {
			continue
		}
		ws.update(e)
	}
}

type snapshot struct {
	event       Event
	hasData     bool
	doneWorkers int
}

func takeSnapshot(workers []*workerState) snapshot {
	var snap snapshot
	for _, w := range workers {
		w.mu.Lock()
		latest := w.latest
		done := w.done
		w.mu.Unlock()

		if latest.Type != "" {
			snap.hasData = true
			snap.event.Total += latest.Total
			snap.event.Failures += latest.Failures
			snap.event.RPS += latest.RPS
			if latest.Elapsed > snap.event.Elapsed {
				snap.event.Elapsed = latest.Elapsed
			}
		}
		if done {
			snap.doneWorkers++
		}
	}
	return snap
}

func mergeLatency(workers []*workerState) map[string]string {
	res := map[string]string{}
	for _, q := range []string{"p50", "p90", "p95", "p99"} {
		var sum, weight float64
		for _, w := range workers {
			w.mu.Lock()
			d := w.latest.Latency[q]
			t := float64(w.latest.Total)
			w.mu.Unlock()
			if d == "" {
				continue
			}
			dur, err := time.ParseDuration(d)
			if err != nil {
				continue
			}
			sum += float64(dur) * t
			weight += t
		}
		if weight > 0 {
			res[q] = time.Duration(sum / weight).String()
		}
	}
	return res
}

// aggregate merges worker events and streams them to the controller's
// subscribers until all workers finish or ctx is canceled.
func aggregate(ctx context.Context, rs *runState, workers []*workerState) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	finished := false
	for {
		snap := takeSnapshot(workers)
		allDone := snap.doneWorkers == len(workers)

		if snap.hasData {
			if allDone {
				snap.event.Type = "done"
				if snap.event.Elapsed > 0 {
					snap.event.RPS = float64(snap.event.Total) / snap.event.Elapsed
				}
				snap.event.Latency = mergeLatency(workers)
				rs.broadcast(snap.event)
				finished = true
				return
			}
			snap.event.Type = "progress"
			rs.broadcast(snap.event)
		} else if allDone {
			// every pod failed; emit an empty done event.
			rs.broadcast(Event{Type: "done", Elapsed: snap.event.Elapsed})
			finished = true
			return
		}

		select {
		case <-ctx.Done():
			if !finished {
				snap = takeSnapshot(workers)
				snap.event.Type = "done"
				if snap.event.Elapsed > 0 {
					snap.event.RPS = float64(snap.event.Total) / snap.event.Elapsed
				}
				snap.event.Latency = mergeLatency(workers)
				rs.broadcast(snap.event)
			}
			return
		case <-ticker.C:
		}
	}
}
