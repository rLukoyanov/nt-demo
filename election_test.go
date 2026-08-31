package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeKube simulates the Kubernetes API for Lease and Pod operations.
type fakeKube struct {
	mu     sync.Mutex
	lease  *lease
	rv     int
	labels map[string]string
}

func (f *fakeKube) resourceVersion() string {
	f.rv++
	return fmt.Sprintf("%d", f.rv)
}

func newFakeKube() *fakeKube {
	return &fakeKube{labels: map[string]string{}}
}

func (f *fakeKube) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.Contains(path, "/leases/") && r.Method == http.MethodGet:
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.lease == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			json.NewEncoder(w).Encode(f.lease)
		case strings.Contains(path, "/leases/") && r.Method == http.MethodPost:
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.lease != nil {
				w.WriteHeader(http.StatusConflict)
				return
			}
			var l lease
			json.NewDecoder(r.Body).Decode(&l)
			l.Metadata.ResourceVersion = f.resourceVersion()
			f.lease = &l
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(f.lease)
		case strings.Contains(path, "/leases/") && r.Method == http.MethodPut:
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.lease == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			var l lease
			json.NewDecoder(r.Body).Decode(&l)
			if l.Metadata.ResourceVersion != f.lease.Metadata.ResourceVersion {
				w.WriteHeader(http.StatusConflict)
				return
			}
			l.Metadata.ResourceVersion = f.resourceVersion()
			f.lease = &l
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(f.lease)
		case strings.Contains(path, "/pods/") && r.Method == http.MethodPatch:
			f.mu.Lock()
			defer f.mu.Unlock()
			var p struct {
				Metadata struct {
					Labels map[string]string `json:"labels"`
				} `json:"metadata"`
			}
			json.NewDecoder(r.Body).Decode(&p)
			for k, v := range p.Metadata.Labels {
				f.labels[k] = v
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

func testClient(srv *httptest.Server) *kubeClient {
	return &kubeClient{
		base:      srv.URL,
		token:     "test",
		namespace: "loadtest",
		http:      srv.Client(),
	}
}

func TestLeaderElectionAcquireAndBlock(t *testing.T) {
	fk := newFakeKube()
	srv := httptest.NewServer(fk.handler())
	defer srv.Close()
	now := func() time.Time { return time.Unix(0, 0) }

	a := &LeaderElector{client: testClient(srv), lease: "l", identity: "pod-a",
		leaseDur: time.Hour, renew: time.Hour, retry: time.Hour, now: now}
	b := &LeaderElector{client: testClient(srv), lease: "l", identity: "pod-b",
		leaseDur: time.Hour, renew: time.Hour, retry: time.Hour, now: now}

	ok, err := a.tryAcquireOrRenew()
	if err != nil || !ok {
		t.Fatalf("a should acquire: ok=%v err=%v", ok, err)
	}
	ok, err = b.tryAcquireOrRenew()
	if err != nil || ok {
		t.Fatalf("b should be blocked: ok=%v err=%v", ok, err)
	}
	ok, err = a.tryAcquireOrRenew()
	if err != nil || !ok {
		t.Fatalf("a should renew its own lease: ok=%v err=%v", ok, err)
	}

	if fk.labels["role"] != "" {
		t.Fatalf("no label patches expected, got %v", fk.labels)
	}
}

func TestLeaderElectionTakeoverAfterExpiry(t *testing.T) {
	fk := newFakeKube()
	srv := httptest.NewServer(fk.handler())
	defer srv.Close()
	at0 := func() time.Time { return time.Unix(0, 0) }
	at30 := func() time.Time { return time.Unix(30, 0) }

	a := &LeaderElector{client: testClient(srv), lease: "l", identity: "pod-a",
		leaseDur: 10 * time.Second, renew: time.Second, retry: time.Second, now: at0}

	// acquire at t=0
	ok, err := a.tryAcquireOrRenew()
	if err != nil || !ok {
		t.Fatalf("a should acquire: ok=%v err=%v", ok, err)
	}

	// advance time past lease expiry (t=30, lease expires at t=10)
	b := &LeaderElector{client: testClient(srv), lease: "l", identity: "pod-b",
		leaseDur: 10 * time.Second, renew: time.Second, retry: time.Second, now: at30}
	ok, err = b.tryAcquireOrRenew()
	if err != nil || !ok {
		t.Fatalf("b should take over after expiry: ok=%v err=%v", ok, err)
	}

	// a is no longer the holder
	ok, err = a.tryAcquireOrRenew()
	if err != nil || ok {
		t.Fatalf("a should have lost leadership: ok=%v err=%v", ok, err)
	}
}

func TestSetLabel(t *testing.T) {
	fk := newFakeKube()
	srv := httptest.NewServer(fk.handler())
	defer srv.Close()

	c := testClient(srv)
	if err := c.setLabel("pod-a", map[string]string{"role": "leader"}); err != nil {
		t.Fatalf("setLabel: %v", err)
	}
	fk.mu.Lock()
	if fk.labels["role"] != "leader" {
		t.Fatalf("expected role=leader, got %v", fk.labels)
	}
	fk.mu.Unlock()
}
