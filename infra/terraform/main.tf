resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  name_prefix             = "sleepy-${var.environment}-${random_id.suffix.hex}"
  generated_registry_name = "sleepy-${var.environment}-${random_id.suffix.hex}"
  registry_name           = var.registry_name != "" ? var.registry_name : local.generated_registry_name
  database_name           = "sleepy"
}

resource "digitalocean_vpc" "main" {
  name     = "${local.name_prefix}-vpc"
  region   = var.region
  ip_range = "10.42.0.0/16"
}

resource "digitalocean_container_registry" "main" {
  count = var.registry_name == "" ? 1 : 0

  name                   = local.registry_name
  subscription_tier_slug = "starter"
  region                 = var.region
}

data "digitalocean_container_registry" "existing" {
  count = var.registry_name != "" ? 1 : 0

  name = var.registry_name
}

resource "digitalocean_container_registry_docker_credentials" "push" {
  registry_name  = local.registry_name
  write          = true
  expiry_seconds = 2592000
}

resource "digitalocean_kubernetes_cluster" "main" {
  name                             = "${local.name_prefix}-doks"
  region                           = var.region
  version                          = var.kubernetes_version
  vpc_uuid                         = digitalocean_vpc.main.id
  registry_integration             = true
  destroy_all_associated_resources = true

  node_pool {
    name       = "default"
    size       = var.kubernetes_node_size
    node_count = 1
  }

  depends_on = [digitalocean_container_registry.main]
}

resource "digitalocean_database_cluster" "postgres" {
  name                 = "${local.name_prefix}-pg"
  engine               = "pg"
  version              = "18"
  size                 = var.postgres_size
  region               = var.region
  node_count           = 1
  private_network_uuid = digitalocean_vpc.main.id
}

resource "digitalocean_database_db" "app" {
  cluster_id = digitalocean_database_cluster.postgres.id
  name       = local.database_name
}

resource "digitalocean_database_firewall" "postgres" {
  cluster_id = digitalocean_database_cluster.postgres.id

  rule {
    type  = "k8s"
    value = digitalocean_kubernetes_cluster.main.id
  }
}
