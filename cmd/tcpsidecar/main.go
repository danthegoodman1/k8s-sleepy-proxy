package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
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
	upstreamAddr  string
	idleAfter     time.Duration
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
	upstreamPort := sleepy.Env("UPSTREAM_PORT", "5432")
	idleSeconds, _ := strconv.Atoi(sleepy.Env("IDLE_SECONDS", "30"))
	if idleSeconds < 5 {
		idleSeconds = 5
	}
	s := &sidecar{
		tenantID:      sleepy.MustEnv("TENANT_ID"),
		controllerURL: sleepy.MustEnv("CONTROLLER_URL"),
		authToken:     sleepy.MustEnv("AUTH_TOKEN"),
		upstreamAddr:  sleepy.Env("UPSTREAM_ADDR", "127.0.0.1:"+upstreamPort),
		idleAfter:     time.Duration(idleSeconds) * time.Second,
		client:        &http.Client{Timeout: 10 * time.Second},
		logger:        logger,
	}
	s.lastTrafficNS.Store(time.Now().UnixNano())
	go s.sleepLoop()

	listenAddr := sleepy.Env("LISTEN_ADDR", ":"+upstreamPort)
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	logger.Info("tcp sidecar listening", "addr", listenAddr, "tenant", s.tenantID, "upstream", s.upstreamAddr, "idleAfter", s.idleAfter.String())
	for {
		conn, err := ln.Accept()
		if err != nil {
			logger.Error("accept", "err", err)
			continue
		}
		go s.proxy(conn)
	}
}

func (s *sidecar) proxy(client net.Conn) {
	s.active.Add(1)
	s.touch()
	defer func() {
		s.touch()
		s.active.Add(-1)
		_ = client.Close()
	}()

	upstream, err := net.DialTimeout("tcp", s.upstreamAddr, 10*time.Second)
	if err != nil {
		s.logger.Error("dial upstream", "tenant", s.tenantID, "upstream", s.upstreamAddr, "err", err)
		return
	}
	defer upstream.Close()

	done := make(chan struct{}, 2)
	go copyAndClose(upstream, client, done)
	go copyAndClose(client, upstream, done)
	<-done
}

func copyAndClose(dst net.Conn, src net.Conn, done chan<- struct{}) {
	_, _ = io.Copy(dst, src)
	if tcp, ok := dst.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}
	done <- struct{}{}
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
		Reason:            "tcp_idle_timeout",
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
	var state sleepy.StateResponse
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return err
	}
	if state.State != sleepy.StateCold {
		return fmt.Errorf("controller returned state %s", state.State)
	}
	return nil
}
