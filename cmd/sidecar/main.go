package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dangoodman/k8s-sleepy-proxy/internal/sleepy"
)

type sidecar struct {
	tenantID      string
	controllerURL string
	authToken     string
	idleAfter     time.Duration
	proxy         *httputil.ReverseProxy
	client        *http.Client
	active        atomic.Int64
	lastTrafficNS atomic.Int64
	sleeping      atomic.Bool
	logger        *slog.Logger
}

func main() {
	cacheWarm := flag.Bool("cache-warm", false, "exit immediately after image pull")
	flag.Parse()
	if *cacheWarm {
		return
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tenantID := sleepy.MustEnv("TENANT_ID")
	upstreamPort := sleepy.Env("UPSTREAM_PORT", "9000")
	upstreamURL := sleepy.Env("UPSTREAM_URL", "http://127.0.0.1:"+upstreamPort)
	target, err := url.Parse(upstreamURL)
	if err != nil {
		log.Fatal(err)
	}
	idleSeconds, _ := strconv.Atoi(sleepy.Env("IDLE_SECONDS", "30"))
	if idleSeconds < 5 {
		idleSeconds = 5
	}
	sc := &sidecar{
		tenantID:      tenantID,
		controllerURL: sleepy.MustEnv("CONTROLLER_URL"),
		authToken:     sleepy.MustEnv("AUTH_TOKEN"),
		idleAfter:     time.Duration(idleSeconds) * time.Second,
		proxy:         httputil.NewSingleHostReverseProxy(target),
		client:        &http.Client{Timeout: 10 * time.Second},
		logger:        logger,
	}
	sc.lastTrafficNS.Store(time.Now().UnixNano())

	go sc.sleepLoop()

	listenAddr := sleepy.Env("LISTEN_ADDR", ":8080")
	logger.Info("sidecar listening", "addr", listenAddr, "tenant", tenantID, "upstream", upstreamURL, "idleAfter", sc.idleAfter.String())
	log.Fatal(http.ListenAndServe(listenAddr, sc))
}

func (s *sidecar) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.active.Add(1)
	s.touch()
	defer func() {
		s.touch()
		s.active.Add(-1)
	}()
	s.proxy.ServeHTTP(w, r)
}

func (s *sidecar) touch() {
	s.lastTrafficNS.Store(time.Now().UnixNano())
}

func (s *sidecar) sleepLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if !s.shouldRequestSleep(time.Now()) {
			continue
		}
		last := time.Unix(0, s.lastTrafficNS.Load())
		s.sleeping.Store(true)
		if err := s.requestSleep(last); err != nil {
			s.sleeping.Store(false)
			s.logger.Error("request sleep", "tenant", s.tenantID, "err", err)
			continue
		}
		s.logger.Info("sleep requested", "tenant", s.tenantID)
	}
}

func (s *sidecar) shouldRequestSleep(now time.Time) bool {
	if s.sleeping.Load() || s.active.Load() != 0 {
		return false
	}
	last := time.Unix(0, s.lastTrafficNS.Load())
	return now.Sub(last) >= s.idleAfter
}

func (s *sidecar) requestSleep(lastTrafficAt time.Time) error {
	payload := sleepy.SleepRequest{
		TenantID:          s.tenantID,
		ActiveConnections: int(s.active.Load()),
		LastTrafficAt:     lastTrafficAt.UTC(),
		Reason:            "idle_timeout",
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, s.controllerURL+"/sleep/"+s.tenantID, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.authToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("controller returned %s", resp.Status)
	}
	return nil
}
