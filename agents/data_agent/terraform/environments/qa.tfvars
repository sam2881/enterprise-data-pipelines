# QA Environment Variables

project_id  = "your-qa-project-id"
region      = "us-central1"
environment = "qa"

# Network
network_id    = ""
subnetwork_id = ""

# Composer
composer_service_account = ""

# BigQuery datasets
bigquery_datasets = ["bronze_qa", "silver_qa", "gold_qa"]

# Labels
labels = {
  managed_by  = "terraform"
  project     = "data-agent"
  environment = "qa"
}
