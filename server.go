package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed static
var staticFS embed.FS

var runCounter atomic.Int64

type runState struct {
	id     string
	cancel context.CancelFunc

	mu   sync.Mutex
	subs map[chan Event]struct{}
}

func (rs *runState) subscribe() chan Event {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	ch := make(chan Event, 64)
	rs.subs[ch] = struct{}{}
	return ch
}

func (rs *runState) unsubscribe(ch chan Event) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	delete(rs.subs, ch)
	close(ch)
}

func (rs *runState) close() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for ch := range rs.subs {
		delete(rs.subs, ch)
		close(ch)
	}
}

// runRequest is the JSON payload accepted by POST /api/run.
type runRequest struct {
	URL         string   `json:"url"`
	Scenario    string   `json:"scenario"`
	Duration    string   `json:"duration"`
	MinRPS      float64  `json:"minRps"`
	MaxRPS      float64  `json:"maxRps"`
	PeakRPS     float64  `json:"peakRps"`
	WavePeriod  string   `json:"period"`
	BaseTime    string   `json:"baseTime"`
	PeakTime    string   `json:"peakTime"`
	Concurrency int      `json:"concurrency"`
	Timeout     string   `json:"timeout"`
	Pods        []string `json:"pods"`
}

type server struct {
	mu   sync.Mutex
	runs map[string]*runState
	pods []string
}

func newServer() *server {
	return &server{runs: map[string]*runState{}, pods: discoverPodsFromEnv()}
}

// discoverPodsFromEnv builds the worker pod list for Kubernetes deployments.
// WORKERS_COUNT + WORKERS_SVC (headless service) + WORKERS_PORT (+ optional
// WORKERS_NAMESPACE) produce http://worker-<i>.<svc>.<ns>:<port> addresses.
// The pod itself (POD_NAME) is excluded: the leader does not generate load.
func discoverPodsFromEnv() []string {
	count := os.Getenv("WORKERS_COUNT")
	if count == "" {
		return nil
	}
	n, err := strconv.Atoi(count)
	if err != nil || n <= 0 {
		return nil
	}
	svc := os.Getenv("WORKERS_SVC")
	if svc == "" {
		return nil
	}
	port := os.Getenv("WORKERS_PORT")
	if port == "" {
		port = "8081"
	}
	ns := os.Getenv("WORKERS_NAMESPACE")
	podName := os.Getenv("POD_NAME")

	pods := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if fmt.Sprintf("worker-%d", i) == podName {
			continue
		}
		host := fmt.Sprintf("worker-%d.%s", i, svc)
		if ns != "" {
			host = fmt.Sprintf("worker-%d.%s.%s", i, svc, ns)
		}
		pods = append(pods, "http://"+host+":"+port)
	}
	return pods
}

func (s *server) get(id string) *runState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runs[id]
}

func (s *server) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Pods) > 0 {
		s.handleDistributedRun(w, req)
		return
	}

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

	// remove run and close subscribers when finished.
	go func() {
		defer func() {
			rs.cancel()
			s.mu.Lock()
			delete(s.runs, id)
			s.mu.Unlock()
			rs.close()
		}()
		if err := Run(ctx, cfg, rs.broadcast); err != nil {
			// ignore; run errors are surfaced through events.
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"id": id})
}

func (rs *runState) broadcast(e Event) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for ch := range rs.subs {
		select {
		case ch <- e:
		default:
			// subscriber is slow or gone; drop event.
		}
	}
}

func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rs := s.get(id)
	if rs == nil {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}
	rs.cancel()
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	rs := s.get(id)
	if rs == nil {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	sub := rs.subscribe()
	defer rs.unsubscribe(sub)

	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			if _, err := fmt.Fprintf(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case e, ok := <-sub:
			if !ok {
				return
			}
			b, _ := json.Marshal(e)
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *server) handlePods(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string][]string{"pods": s.pods})
}

func cfgFromRequest(req runRequest) (*Config, error) {
	dur, err := time.ParseDuration(req.Duration)
	if err != nil {
		return nil, fmt.Errorf("invalid duration: %v", err)
	}
	if dur <= 0 {
		return nil, fmt.Errorf("duration must be positive")
	}
	if req.URL == "" {
		return nil, fmt.Errorf("url is required")
	}
	if req.Scenario == "" {
		req.Scenario = "linear"
	}
	if req.Concurrency <= 0 {
		req.Concurrency = 50
	}
	var period, timeout, baseTime, peakTime time.Duration
	if req.WavePeriod != "" {
		period, err = time.ParseDuration(req.WavePeriod)
		if err != nil {
			return nil, fmt.Errorf("invalid period: %v", err)
		}
	}
	if req.BaseTime != "" {
		baseTime, err = time.ParseDuration(req.BaseTime)
		if err != nil {
			return nil, fmt.Errorf("invalid base-time: %v", err)
		}
	}
	if req.PeakTime != "" {
		peakTime, err = time.ParseDuration(req.PeakTime)
		if err != nil {
			return nil, fmt.Errorf("invalid peak-time: %v", err)
		}
	}
	if req.Timeout != "" {
		timeout, err = time.ParseDuration(req.Timeout)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout: %v", err)
		}
	}
	return &Config{
		URL:         req.URL,
		Scenario:    req.Scenario,
		Duration:    dur,
		MinRPS:      req.MinRPS,
		MaxRPS:      req.MaxRPS,
		PeakRPS:     req.PeakRPS,
		WavePeriod:  period,
		BaseTime:    baseTime,
		PeakTime:    peakTime,
		Concurrency: req.Concurrency,
		Timeout:     timeout,
	}, nil
}

func startServer(addr string) error {
	return serve(addr, "Web UI")
}

func startWorker(addr string) error {
	return serve(addr, "Worker")
}

func serve(addr, label string) error {
	h, err := newServerHandler()
	if err != nil {
		return err
	}
	fmt.Printf("%s: http://%s\n", label, addr)
	return http.ListenAndServe(addr, h)
}

func newServerHandler() (http.Handler, error) {
	s := newServer()

	staticDir, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServer(http.FS(staticDir)))
	mux.HandleFunc("POST /api/run", s.handleRun)
	mux.HandleFunc("POST /api/run/{id}/cancel", s.handleCancel)
	mux.HandleFunc("GET /api/stream", s.handleStream)
	mux.HandleFunc("GET /api/pods", s.handlePods)
	return mux, nil
}
