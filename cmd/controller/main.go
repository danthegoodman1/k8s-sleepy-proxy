package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/dangoodman/k8s-sleepy-proxy/internal/controller"
	"github.com/dangoodman/k8s-sleepy-proxy/internal/sleepy"
	"github.com/dangoodman/k8s-sleepy-proxy/internal/store"
)

func main() {
	cacheWarm := flag.Bool("cache-warm", false, "exit immediately after image pull")
	flag.Parse()
	if *cacheWarm {
		return
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	databaseURL := sleepy.MustEnv("DATABASE_URL")
	authToken := sleepy.MustEnv("AUTH_TOKEN")
	sidecarImage := sleepy.MustEnv("SIDECAR_IMAGE")
	namespace := sleepy.Env("NAMESPACE", "sleepy-system")
	listenAddr := sleepy.Env("LISTEN_ADDR", ":8080")
	secretName := sleepy.Env("SECRET_NAME", "sleepy-secrets")
	controllerURL := sleepy.Env("CONTROLLER_URL_FOR_SIDECAR", "http://sleepy-controller.sleepy-system.svc.cluster.local:8080")
	wakeTimeout := sleepy.EnvDurationSeconds("WAKE_TIMEOUT_SECONDS", 120*time.Second)
	volume := controller.TenantVolumeConfig{
		Enabled:          true,
		ClaimName:        sleepy.Env("TENANT_VOLUME_CLAIM_NAME", "data"),
		MountPath:        sleepy.Env("TENANT_VOLUME_MOUNT_PATH", "/data"),
		StorageClassName: sleepy.Env("TENANT_VOLUME_STORAGE_CLASS", "archil"),
		Size:             sleepy.Env("TENANT_VOLUME_SIZE", "1Gi"),
		ProvisionTimeout: sleepy.EnvDurationSeconds("TENANT_VOLUME_PROVISION_TIMEOUT_SECONDS", 60*time.Second),
	}

	tenantStore, err := store.NewPostgresStore(databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer tenantStore.Close()
	if err := tenantStore.EnsureSchema(context.Background()); err != nil {
		log.Fatal(err)
	}

	kube, err := controller.NewKubernetesManager(namespace, sidecarImage, controllerURL, secretName, volume)
	if err != nil {
		log.Fatal(err)
	}

	server := controller.NewServer(tenantStore, kube, wakeTimeout, logger)
	logger.Info("controller listening", "addr", listenAddr, "namespace", namespace)
	log.Fatal(http.ListenAndServe(listenAddr, sleepy.RequireBearer(authToken, server)))
}
