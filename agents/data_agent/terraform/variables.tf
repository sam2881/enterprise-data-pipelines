# Enterprise Agentic Data Platform - Terraform Variables

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment (dev, qa, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "qa", "prod"], var.environment)
    error_message = "Environment must be one of: dev, qa, prod"
  }
}

variable "network_id" {
  description = "VPC Network ID"
  type        = string
  default     = ""
}

variable "subnetwork_id" {
  description = "VPC Subnetwork ID"
  type        = string
  default     = ""
}

variable "composer_service_account" {
  description = "Service account for Composer"
  type        = string
  default     = ""
}

variable "bigquery_datasets" {
  description = "List of BigQuery datasets to create"
  type        = list(string)
  default     = ["bronze", "silver", "gold"]
}

variable "labels" {
  description = "Labels to apply to all resources"
  type        = map(string)
  default = {
    managed_by = "terraform"
    project    = "data-agent"
  }
}
