package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/dangoodman/k8s-sleepy-proxy/internal/sleepy"
)

const (
	postgresProtocolVersion = 196608
	postgresSSLRequest      = 80877103
)

type cacheEntry struct {
	state   sleepy.StateResponse
	expires time.Time
}

type server struct {
	controllerURL string
	authToken     string
	client        *http.Client
	cacheTTL      time.Duration
	wakeTimeout   time.Duration
	mu            sync.Mutex
	cache         map[string]cacheEntry
	logger        *slog.Logger
}

type startupPacket struct {
	tenantID string
	payload  []byte
}

func main() {
	cacheWarm := flag.Bool("cache-warm", false, "exit immediately after image pull")
	flag.Parse()
	if *cacheWarm {
		return
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	s := &server{
		controllerURL: sleepy.Env("CONTROLLER_URL", "http://sleepy-controller.sleepy-system.svc.cluster.local:8080"),
		authToken:     sleepy.MustEnv("AUTH_TOKEN"),
		client:        &http.Client{Timeout: 130 * time.Second},
		cacheTTL:      sleepy.EnvDurationSeconds("CACHE_TTL_SECONDS", 5*time.Second),
		wakeTimeout:   sleepy.EnvDurationSeconds("WAKE_TIMEOUT_SECONDS", 120*time.Second),
		cache:         map[string]cacheEntry{},
		logger:        logger,
	}
	listenAddr := sleepy.Env("LISTEN_ADDR", ":5432")
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	logger.Info("tcp lb listening", "addr", listenAddr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			logger.Error("accept", "err", err)
			continue
		}
		go s.handle(conn)
	}
}

func (s *server) handle(client net.Conn) {
	defer client.Close()

	started := time.Now()
	packet, err := readPostgresStartup(client)
	if err != nil {
		s.logger.Error("read startup", "err", err)
		return
	}
	state, source, err := s.resolveBackend(context.Background(), packet.tenantID)
	if err != nil {
		s.logger.Error("resolve backend", "tenant", packet.tenantID, "err", err)
		return
	}
	if state.State != sleepy.StateRunning || state.Backend == "" {
		s.logger.Error("tenant unavailable", "tenant", packet.tenantID, "state", state.State)
		return
	}

	backend, err := net.DialTimeout("tcp", state.Backend, 10*time.Second)
	if err != nil {
		s.evict(packet.tenantID)
		s.logger.Error("dial backend", "tenant", packet.tenantID, "backend", state.Backend, "err", err)
		return
	}
	defer backend.Close()
	if _, err := backend.Write(packet.payload); err != nil {
		s.evict(packet.tenantID)
		s.logger.Error("write startup", "tenant", packet.tenantID, "backend", state.Backend, "err", err)
		return
	}

	s.logger.Info("tcp route", "tenant", packet.tenantID, "source", source, "backend", state.Backend, "resolveDuration", time.Since(started).String())
	done := make(chan struct{}, 2)
	go copyAndClose(backend, client, done)
	go copyAndClose(client, backend, done)
	<-done
}

func readPostgresStartup(conn net.Conn) (startupPacket, error) {
	for attempts := 0; attempts < 2; attempts++ {
		payload, code, err := readStartupPayload(conn)
		if err != nil {
			return startupPacket{}, err
		}
		if code == postgresSSLRequest {
			if _, err := conn.Write([]byte("N")); err != nil {
				return startupPacket{}, err
			}
			continue
		}
		if code != postgresProtocolVersion {
			return startupPacket{}, fmt.Errorf("unsupported postgres startup code %d", code)
		}
		tenantID, err := tenantFromStartup(payload[8:])
		if err != nil {
			return startupPacket{}, err
		}
		return startupPacket{tenantID: tenantID, payload: payload}, nil
	}
	return startupPacket{}, errors.New("client did not send postgres startup after SSL denial")
}

func readStartupPayload(r io.Reader) ([]byte, uint32, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, 0, err
	}
	packetLen := int(binary.BigEndian.Uint32(lenBuf[:]))
	if packetLen < 8 || packetLen > 1<<20 {
		return nil, 0, fmt.Errorf("invalid postgres startup length %d", packetLen)
	}
	payload := make([]byte, packetLen)
	copy(payload[:4], lenBuf[:])
	if _, err := io.ReadFull(r, payload[4:]); err != nil {
		return nil, 0, err
	}
	return payload, binary.BigEndian.Uint32(payload[4:8]), nil
}

func tenantFromStartup(params []byte) (string, error) {
	values := map[string]string{}
	parts := bytes.Split(params, []byte{0})
	for i := 0; i+1 < len(parts); i += 2 {
		key := string(parts[i])
		if key == "" {
			break
		}
		values[key] = string(parts[i+1])
	}
	for _, key := range []string{"database", "user"} {
		value := values[key]
		if value == "" || value == "postgres" {
			continue
		}
		if err := sleepy.ValidateTenantID(value); err == nil {
			return value, nil
		}
	}
	return "", fmt.Errorf("startup packet did not include a tenant database or user")
}

func copyAndClose(dst net.Conn, src net.Conn, done chan<- struct{}) {
	_, _ = io.Copy(dst, src)
	if tcp, ok := dst.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}
	done <- struct{}{}
}

func (s *server) resolveBackend(ctx context.Context, tenantID string) (sleepy.StateResponse, string, error) {
	if cached, ok := s.cached(tenantID); ok && cached.State == sleepy.StateRunning && cached.Backend != "" {
		return cached, "memory_cache", nil
	}
	state, err := s.state(ctx, tenantID)
	if err != nil {
		return sleepy.StateResponse{}, "", err
	}
	switch state.State {
	case sleepy.StateRunning:
		s.putCache(state)
		return state, "controller_state", nil
	case sleepy.StateCold, sleepy.StateFailed:
		ctx, cancel := context.WithTimeout(ctx, s.wakeTimeout)
		defer cancel()
		state, err = s.wake(ctx, tenantID)
		if err != nil {
			return sleepy.StateResponse{}, "", err
		}
		if state.State != sleepy.StateRunning || state.Backend == "" {
			return state, "", fmt.Errorf("wake returned %s: %s", state.State, state.FailureReason)
		}
		s.putCache(state)
		return state, "controller_wake", nil
	case sleepy.StateWaking, sleepy.StateDraining:
		state, err := s.pollRunning(ctx, tenantID)
		if err != nil {
			return sleepy.StateResponse{}, "", err
		}
		return state, "controller_poll", nil
	default:
		return sleepy.StateResponse{}, "", fmt.Errorf("unknown tenant state %q", state.State)
	}
}

func (s *server) pollRunning(ctx context.Context, tenantID string) (sleepy.StateResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.wakeTimeout)
	defer cancel()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		state, err := s.state(ctx, tenantID)
		if err != nil {
			return sleepy.StateResponse{}, err
		}
		if state.State == sleepy.StateRunning && state.Backend != "" {
			s.putCache(state)
			return state, nil
		}
		if state.State == sleepy.StateFailed {
			return state, fmt.Errorf("tenant failed: %s", state.FailureReason)
		}
		select {
		case <-ctx.Done():
			return sleepy.StateResponse{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *server) state(ctx context.Context, tenantID string) (sleepy.StateResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.controllerURL+"/state/"+tenantID, nil)
	if err != nil {
		return sleepy.StateResponse{}, err
	}
	req.Header.Set("Authorization", "Bearer "+s.authToken)
	return s.doState(req)
}

func (s *server) wake(ctx context.Context, tenantID string) (sleepy.StateResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.controllerURL+"/wake/"+tenantID, bytes.NewReader(nil))
	if err != nil {
		return sleepy.StateResponse{}, err
	}
	req.Header.Set("Authorization", "Bearer "+s.authToken)
	return s.doState(req)
}

func (s *server) doState(req *http.Request) (sleepy.StateResponse, error) {
	resp, err := s.client.Do(req)
	if err != nil {
		return sleepy.StateResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return sleepy.StateResponse{}, fmt.Errorf("%s returned %s", req.URL.Path, resp.Status)
	}
	var state sleepy.StateResponse
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return sleepy.StateResponse{}, err
	}
	return state, nil
}

func (s *server) cached(tenantID string) (sleepy.StateResponse, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.cache[tenantID]
	if !ok || time.Now().After(entry.expires) {
		delete(s.cache, tenantID)
		return sleepy.StateResponse{}, false
	}
	return entry.state, true
}

func (s *server) putCache(state sleepy.StateResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache[state.TenantID] = cacheEntry{state: state, expires: time.Now().Add(s.cacheTTL)}
}

func (s *server) evict(tenantID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cache, tenantID)
}
