# k8s-sleepy-proxy

Minimal DigitalOcean Kubernetes PoC for a "sleepy proxy": tenant workloads are
created on first HTTP request, removed after idleness, and recreated from
Postgres when traffic returns.

## Prerequisites

- Terraform
- Docker with buildx
- kubectl
- Go, for local tests
- `.env.local` containing `DO_API_KEY=...` and `ARCHIL_API_KEY=...`

`doctl` is not required.

You can start from:

```sh
cp .env.local.example .env.local
```

## Quickstart

```sh
make infra-up
make images-push
make deploy
make seed-tenant
make demo
make demo-archil
```

The demo sends requests with:

```sh
curl -H "TENANT: tenant-a" http://<sleepy-lb-ip>/hello
```

## Watch Cold vs Hot Routing

The deploy includes a `sleepy-image-cache` DaemonSet that pre-pulls the
controller, LB, sidecar, and echo images onto every node. Tenant pods use the
same DOCR pull secret and default `IfNotPresent` image pulls, so cold tenant
starts should use the node-local image cache.

```sh
kubectl --kubeconfig .generated/kubeconfig -n sleepy-system logs -f deployment/sleepy-lb
kubectl --kubeconfig .generated/kubeconfig -n sleepy-system logs -f deployment/sleepy-controller
```

The LB logs `source=controller_wake` for a DB-backed cold wake and
`source=memory_cache` for a hot in-memory route.

```sh
curl -fsS -w '\ntime_total=%{time_total}\n' -H 'TENANT: tenant-a' http://<sleepy-lb-ip>/cold
curl -fsS -w '\ntime_total=%{time_total}\n' -H 'TENANT: tenant-a' http://<sleepy-lb-ip>/hot
kubectl --kubeconfig .generated/kubeconfig -n sleepy-system describe pod sleepy-tenant-a-0
```

In the pod events, look for `already present on machine` for both the echo app
and sidecar images.

## Archil Disk Demo

`make deploy` installs the Archil CSI driver with token-based dynamic
provisioning and creates the `archil` StorageClass. Registering a tenant creates
its PVC immediately; the tenant StatefulSet also declares that same
volumeClaimTemplate and mounts it at `/data`.

```sh
make seed-tenant
make demo-archil
```

The `/incr` route increments `/data/count.txt` and calls `fsync()` before
responding. The demo checks cold `/incr`, hot `/incr`, sleep cleanup, then cold
`/incr` again to confirm the count continues from the same Archil disk.

## Destroy

```sh
make destroy
```

This deletes the Kubernetes app resources and then destroys the Terraform-managed
DigitalOcean resources.

## Notes

- Terraform state is local and ignored because it contains secrets.
- If the team already has a DigitalOcean Container Registry, scripts reuse it;
  otherwise Terraform creates one. Images use one repository with multiple tags,
  which fits the free Starter registry tier.
- The controller owns the Postgres schema migration for this PoC.
- There is intentionally no CRD; the controller directly creates and deletes the
  tenant `Service` and `StatefulSet`.
