package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dangoodman/k8s-sleepy-proxy/internal/sleepy"
)

func main() {
	cacheWarm := flag.Bool("cache-warm", false, "exit immediately after image pull")
	flag.Parse()
	if *cacheWarm {
		return
	}

	listenAddr := sleepy.Env("LISTEN_ADDR", ":9000")
	dataDir := sleepy.Env("DATA_DIR", "/data")
	http.HandleFunc("/incr", func(w http.ResponseWriter, r *http.Request) {
		count, err := incrementCounter(filepath.Join(dataDir, "count.txt"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"service":   "sleepy-echo",
			"path":      r.URL.Path,
			"tenant":    r.Header.Get("X-Sleepy-Tenant"),
			"hostname":  hostname(),
			"count":     count,
			"file":      filepath.Join(dataDir, "count.txt"),
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"service":   "sleepy-echo",
			"method":    r.Method,
			"path":      r.URL.Path,
			"tenant":    r.Header.Get("X-Sleepy-Tenant"),
			"hostname":  hostname(),
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	})
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}

func incrementCounter(path string) (int, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return 0, err
	}
	defer func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	}()

	raw, err := io.ReadAll(f)
	if err != nil {
		return 0, err
	}
	count := 0
	text := strings.TrimSpace(string(raw))
	if text != "" {
		count, err = strconv.Atoi(text)
		if err != nil {
			return 0, fmt.Errorf("parse %s: %w", path, err)
		}
	}
	count++
	if err := f.Truncate(0); err != nil {
		return 0, err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return 0, err
	}
	if _, err := fmt.Fprintf(f, "%d\n", count); err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	return count, nil
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return ""
	}
	return name
}
