variable "do_token" {
  description = "DigitalOcean API token. scripts map DO_API_KEY from .env.local to this variable."
  type        = string
  sensitive   = true
}

variable "environment" {
  description = "Short environment name used in resource names."
  type        = string
  default     = "poc"
}

variable "region" {
  description = "DigitalOcean region."
  type        = string
  default     = "sfo3"
}

variable "registry_name" {
  description = "Existing DigitalOcean Container Registry name. Leave empty to create one."
  type        = string
  default     = ""
}

variable "kubernetes_node_size" {
  description = "DOKS worker node size."
  type        = string
  default     = "s-2vcpu-4gb"
}

variable "kubernetes_version" {
  description = "DOKS version for the PoC cluster."
  type        = string
  default     = "1.35.1-do.5"
}

variable "postgres_size" {
  description = "Managed Postgres node size."
  type        = string
  default     = "db-s-1vcpu-1gb"
}
