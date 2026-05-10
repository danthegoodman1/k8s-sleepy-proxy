output "cluster_name" {
  value = digitalocean_kubernetes_cluster.main.name
}

output "cluster_id" {
  value = digitalocean_kubernetes_cluster.main.id
}

output "kubeconfig" {
  value     = digitalocean_kubernetes_cluster.main.kube_config[0].raw_config
  sensitive = true
}

output "region" {
  value = var.region
}

output "registry_name" {
  value = local.registry_name
}

output "registry_endpoint" {
  value = "registry.digitalocean.com/${local.registry_name}"
}

output "registry_docker_credentials" {
  value     = digitalocean_container_registry_docker_credentials.push.docker_credentials
  sensitive = true
}

output "postgres_private_uri" {
  value = format(
    "postgresql://%s:%s@%s:%d/%s?sslmode=require",
    urlencode(digitalocean_database_cluster.postgres.user),
    urlencode(digitalocean_database_cluster.postgres.password),
    digitalocean_database_cluster.postgres.private_host,
    digitalocean_database_cluster.postgres.port,
    digitalocean_database_db.app.name,
  )
  sensitive = true
}
