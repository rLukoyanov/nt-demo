package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	saDir      = "/var/run/secrets/kubernetes.io/serviceaccount"
	leasePath  = "/apis/coordination.k8s.io/v1/namespaces/%s/leases/%s"
	podPath    = "/api/v1/namespaces/%s/pods/%s"
	leaseName  = "nt-demo-leader"
	roleLeader = "leader"
	roleWorker = "worker"
)

// kubeClient talks to the Kubernetes API using the in-cluster service account.
type kubeClient struct {
	base      string
	token     string
	namespace string
	http      *http.Client
}

func inClusterClient() (*kubeClient, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("KUBERNETES_SERVICE_HOST/PORT not set (run in-cluster)")
	}
	token, err := os.ReadFile(saDir + "/token")
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	ns, err := os.ReadFile(saDir + "/namespace")
	if err != nil {
		return nil, fmt.Errorf("read namespace: %w", err)
	}
	ca, err := os.ReadFile(saDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read ca.crt: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	}
	return &kubeClient{
		base:      "https://" + host + ":" + port,
		token:     strings.TrimSpace(string(token)),
		namespace: strings.TrimSpace(string(ns)),
		http:      &http.Client{Transport: transport, Timeout: 5 * time.Second},
	}, nil
}

func (c *kubeClient) do(method, path string, body []byte, contentType string) (*http.Response, error) {
	req, err := http.NewRequest(method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	} else if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.http.Do(req)
}

func (c *kubeClient) setLabel(pod string, labels map[string]string) error {
	payload := map[string]interface{}{"metadata": map[string]interface{}{"labels": labels}}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, err := c.do(http.MethodPatch, fmt.Sprintf(podPath, c.namespace, pod), b, "application/merge-patch+json")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("patch labels: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

type metaObj struct {
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
}

type leaseSpec struct {
	HolderIdentity       string `json:"holderIdentity,omitempty"`
	LeaseDurationSeconds *int32 `json:"leaseDurationSeconds,omitempty"`
	AcquireTime          string `json:"acquireTime,omitempty"`
	RenewTime            string `json:"renewTime,omitempty"`
	LeaderTransitions    *int32 `json:"leaderTransitions,omitempty"`
}

type lease struct {
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Metadata   metaObj   `json:"metadata"`
	Spec       leaseSpec `json:"spec"`
}

// LeaderElector acquires and renews a Kubernetes Lease to elect a single leader
// among identical pods.
type LeaderElector struct {
	client   *kubeClient
	lease    string
	identity string
	leaseDur time.Duration
	renew    time.Duration
	retry    time.Duration
	now      func() time.Time
}

func (le *LeaderElector) nowFunc() func() time.Time {
	if le.now == nil {
		return time.Now
	}
	return le.now
}

func (le *LeaderElector) leaseURL() string {
	return fmt.Sprintf(leasePath, le.client.namespace, le.lease)
}

// leasesURL is the collection endpoint used to create a Lease (POST).
func (le *LeaderElector) leasesURL() string {
	return fmt.Sprintf("/apis/coordination.k8s.io/v1/namespaces/%s/leases", le.client.namespace)
}

// tryAcquireOrRenew returns true if this instance currently holds the lease.
func (le *LeaderElector) tryAcquireOrRenew() (bool, error) {
	return le.tryAcquireOrRenewAt(le.nowFunc()())
}

func (le *LeaderElector) tryAcquireOrRenewAt(at time.Time) (bool, error) {
	// Kubernetes metav1.Time parses RFC3339 with up to 6 fractional digits.
	now := at.UTC().Format("2006-01-02T15:04:05.000000Z07:00")
	resp, err := le.client.do(http.MethodGet, le.leaseURL(), nil, "")
	if err != nil {
		return false, err
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return le.createLease(now)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return false, fmt.Errorf("get lease: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var l lease
	if err := json.NewDecoder(resp.Body).Decode(&l); err != nil {
		resp.Body.Close()
		return false, err
	}
	resp.Body.Close()

	renew, err := time.Parse(time.RFC3339Nano, l.Spec.RenewTime)
	if err != nil {
		renew = time.Time{}
	}
	expired := l.Spec.HolderIdentity == "" || renew.IsZero() || at.After(renew.Add(le.leaseDur))

	if expired {
		return le.updateLease(l, now, true)
	}
	if l.Spec.HolderIdentity == le.identity {
		return le.updateLease(l, now, false)
	}
	return false, nil
}

func (le *LeaderElector) createLease(now string) (bool, error) {
	dur := int32(le.leaseDur / time.Second)
	trans := int32(0)
	body, _ := json.Marshal(lease{
		APIVersion: "coordination.k8s.io/v1",
		Kind:       "Lease",
		Metadata:   metaObj{Name: le.lease, Namespace: le.client.namespace},
		Spec: leaseSpec{
			HolderIdentity:       le.identity,
			LeaseDurationSeconds: &dur,
			AcquireTime:          now,
			RenewTime:            now,
			LeaderTransitions:    &trans,
		},
	})
	resp, err := le.client.do(http.MethodPost, le.leasesURL(), body, "")
	if err != nil {
		return false, err
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusCreated:
		return true, nil
	case http.StatusConflict:
		return false, nil
	default:
		return false, fmt.Errorf("create lease: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
}

func (le *LeaderElector) updateLease(l lease, now string, takeover bool) (bool, error) {
	l.Spec.HolderIdentity = le.identity
	l.Spec.RenewTime = now
	dur := int32(le.leaseDur / time.Second)
	l.Spec.LeaseDurationSeconds = &dur
	if takeover {
		l.Spec.AcquireTime = now
		trans := int32(1)
		if l.Spec.LeaderTransitions != nil {
			trans = *l.Spec.LeaderTransitions + 1
		}
		l.Spec.LeaderTransitions = &trans
	}
	body, _ := json.Marshal(l)
	resp, err := le.client.do(http.MethodPut, le.leaseURL(), body, "")
	if err != nil {
		return false, err
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusConflict:
		// resourceVersion changed since our GET: we lost the race.
		return false, nil
	default:
		return false, fmt.Errorf("update lease: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
}

// Run acquires the lease, calls onStartedLeading, then renews it. If the lease
// is lost, onStoppedLeading is called and acquisition is retried. Returns when
// ctx is canceled.
func (le *LeaderElector) Run(ctx context.Context, onStartedLeading func(), onStoppedLeading func()) error {
	for {
		// acquire
		retry := time.NewTicker(le.retry)
		acquired := false
		for !acquired {
			ok, err := le.tryAcquireOrRenew()
			if err != nil {
				fmt.Fprintf(os.Stderr, "election: %v\n", err)
			} else if ok {
				acquired = true
				break
			}
			select {
			case <-ctx.Done():
				retry.Stop()
				return ctx.Err()
			case <-retry.C:
			}
		}
		retry.Stop()

		onStartedLeading()

		// maintain
		renewTicker := time.NewTicker(le.renew)
		deadline := time.Now().Add(le.leaseDur)
		maintained := true
		for maintained {
			select {
			case <-ctx.Done():
				renewTicker.Stop()
				onStoppedLeading()
				return ctx.Err()
			case <-renewTicker.C:
				ok, err := le.tryAcquireOrRenew()
				if err != nil {
					fmt.Fprintf(os.Stderr, "election renew: %v\n", err)
				} else if ok {
					deadline = time.Now().Add(le.leaseDur)
					continue
				}
				if time.Now().After(deadline) {
					renewTicker.Stop()
					onStoppedLeading()
					maintained = false
				}
			}
		}
	}
}

// runElected runs a leader-elected pod. All pods start as workers; the elected
// leader stops its worker server and serves only the controller UI (it does not
// generate load). A dedicated health endpoint keeps readiness probes green.
func runElected(ctx context.Context, workerAddr, serverAddr string) error {
	client, err := inClusterClient()
	if err != nil {
		return err
	}
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		return fmt.Errorf("POD_NAME env is required")
	}
	name := os.Getenv("LEASE_NAME")
	if name == "" {
		name = leaseName
	}
	healthAddr := os.Getenv("WORKERS_HEALTH_PORT")
	if healthAddr == "" {
		healthAddr = ":9090"
	}

	// always-on health endpoint for the readiness probe (both roles).
	go func() {
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		if err := http.ListenAndServe(healthAddr, h); err != nil {
			fmt.Fprintf(os.Stderr, "health: %v\n", err)
		}
	}()

	var workerSrv, ctrl *http.Server
	var srvMu sync.Mutex

	startSrv := func(srv **http.Server, addr string) {
		srvMu.Lock()
		defer srvMu.Unlock()
		if *srv != nil {
			return
		}
		h, err := newServerHandler()
		if err != nil {
			fmt.Fprintf(os.Stderr, "handler for %s: %v\n", addr, err)
			return
		}
		s := &http.Server{Addr: addr, Handler: h}
		*srv = s
		go func() {
			if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Fprintf(os.Stderr, "server %s: %v\n", addr, err)
			}
		}()
	}

	stopSrv := func(srv **http.Server) {
		srvMu.Lock()
		defer srvMu.Unlock()
		if *srv != nil {
			shCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			_ = (*srv).Shutdown(shCtx)
			cancel()
			*srv = nil
		}
	}

	// all pods start as workers; the leader switches to controller-only.
	startSrv(&workerSrv, workerAddr)

	startCtrl := func() {
		if err := client.setLabel(podName, map[string]string{"role": roleLeader}); err != nil {
			fmt.Fprintf(os.Stderr, "set role=leader: %v\n", err)
		}
		stopSrv(&workerSrv)
		startSrv(&ctrl, serverAddr)
		fmt.Printf("Elected leader %s, serving UI on %s (load generated by other pods only)\n", podName, serverAddr)
	}

	stopCtrl := func() {
		if err := client.setLabel(podName, map[string]string{"role": roleWorker}); err != nil {
			fmt.Fprintf(os.Stderr, "set role=worker: %v\n", err)
		}
		stopSrv(&ctrl)
		startSrv(&workerSrv, workerAddr)
		fmt.Printf("Leadership lost, serving as worker on %s\n", podName)
	}

	le := &LeaderElector{
		client:   client,
		lease:    name,
		identity: podName,
		leaseDur: 15 * time.Second,
		renew:    3 * time.Second,
		retry:    2 * time.Second,
	}
	return le.Run(ctx, startCtrl, stopCtrl)
}
