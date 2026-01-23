# Production Environment Variables

project_id  = "your-prod-project-id"
region      = "us-central1"
environment = "prod"

# Network
network_id    = ""
subnetwork_id = ""

# Composer
composer_service_account = ""

# BigQuery datasets
bigquery_datasets = ["bronze", "silver", "gold"]

# Labels
labels = {
  managed_by  = "terraform"
  project     = "data-agent"
  environment = "prod"
}
