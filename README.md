# k8s-sleepy-proxy

Minimal DigitalOcean Kubernetes PoC for a "sleepy proxy": tenant workloads are
created on first HTTP request, removed after idleness, and recreated from
Postgres when traffic returns.

## Prerequisites

- Terraform
- Docker with buildx
- kubectl
- Go, for local tests
- `.env.local` containing `DO_API_KEY=...`

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
```

The demo sends requests with:

```sh
curl -H "TENANT: tenant-a" http://<sleepy-lb-ip>/hello
```

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
