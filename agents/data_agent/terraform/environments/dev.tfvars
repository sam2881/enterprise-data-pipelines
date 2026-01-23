# Development Environment Variables

project_id  = "your-dev-project-id"
region      = "us-central1"
environment = "dev"

# Network (use default or specify custom)
network_id    = ""
subnetwork_id = ""

# Composer
composer_service_account = ""

# BigQuery datasets
bigquery_datasets = ["bronze_dev", "silver_dev", "gold_dev"]

# Labels
labels = {
  managed_by  = "terraform"
  project     = "data-agent"
  environment = "dev"
}
