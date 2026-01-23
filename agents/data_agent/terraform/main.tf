# Enterprise Agentic Data Platform - Main Terraform Configuration
# Placeholder - implement infrastructure in Phase 8

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
  }

  # Backend configuration - uncomment and configure for state management
  # backend "gcs" {
  #   bucket = "terraform-state-bucket"
  #   prefix = "data-agent"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# Cloud SQL PostgreSQL Instance
module "cloudsql" {
  source = "./modules/cloudsql"

  project_id    = var.project_id
  region        = var.region
  environment   = var.environment
  database_name = "data_agent_${var.environment}"
}

# Cloud Composer Environment
module "composer" {
  source = "./modules/composer"

  project_id       = var.project_id
  region           = var.region
  environment      = var.environment
  network_id       = var.network_id
  subnetwork_id    = var.subnetwork_id
  service_account  = var.composer_service_account
}

# BigQuery Dataset
module "bigquery" {
  source = "./modules/bigquery"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment
  datasets    = var.bigquery_datasets
}

# Pub/Sub Topics and Subscriptions
module "pubsub" {
  source = "./modules/pubsub"

  project_id  = var.project_id
  environment = var.environment
}

# Outputs
output "cloudsql_connection_name" {
  description = "Cloud SQL connection name"
  value       = module.cloudsql.connection_name
}

output "composer_dags_bucket" {
  description = "Composer DAGs bucket"
  value       = module.composer.dags_bucket
}

output "bigquery_datasets" {
  description = "BigQuery datasets created"
  value       = module.bigquery.dataset_ids
}
