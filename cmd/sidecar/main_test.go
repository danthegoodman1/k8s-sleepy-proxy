package main

import (
	"testing"
	"time"
)

func TestSidecarDoesNotSleepWhileRequestActive(t *testing.T) {
	sc := &sidecar{idleAfter: time.Second}
	sc.lastTrafficNS.Store(time.Now().Add(-time.Hour).UnixNano())
	sc.active.Store(1)
	if sc.shouldRequestSleep(time.Now()) {
		t.Fatal("sidecar should not sleep while a request is active")
	}
	sc.active.Store(0)
	if !sc.shouldRequestSleep(time.Now()) {
		t.Fatal("sidecar should sleep after idle timeout with no active requests")
	}
}
