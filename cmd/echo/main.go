package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
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

func hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return ""
	}
	return name
}
