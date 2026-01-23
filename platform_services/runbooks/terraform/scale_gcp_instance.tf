# ==============================================================================
# METADATA
# ==============================================================================
# name: Scale GCP Compute Instance
# description: Resize GCP VM instance to handle increased load
# service: gcp
# component: compute
# action: scale
# risk: medium
# requires_approval: true
# estimated_time_minutes: 15
# tags: [gcp, vm, scale, compute, resize]
# error_patterns:
#   - "instance.*cpu.*high"
#   - "vm.*overloaded"
#   - "compute.*throttling"
# keywords: [gcp, vm, instance, scale, resize, cpu, memory, compute]
# pre_checks: [verify_instance_exists, capture_current_size]
# post_checks: [verify_new_size, check_instance_health]
# rollback_script: terraform/rollback_instance_scale.tf
# ==============================================================================

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "zone" {
  description = "GCP Zone"
  type        = string
  default     = "us-central1-a"
}

variable "instance_name" {
  description = "Name of the instance to scale"
  type        = string
}

variable "new_machine_type" {
  description = "New machine type for scaling"
  type        = string
  default     = "e2-standard-4"
}

provider "google" {
  project = var.project_id
  region  = "us-central1"
}

# Data source to get current instance
data "google_compute_instance" "target" {
  name    = var.instance_name
  zone    = var.zone
}

# Stop instance before resize (required for machine type change)
resource "null_resource" "stop_instance" {
  provisioner "local-exec" {
    command = "gcloud compute instances stop ${var.instance_name} --zone=${var.zone} --project=${var.project_id}"
  }

  triggers = {
    machine_type = var.new_machine_type
  }
}

# Change machine type
resource "null_resource" "resize_instance" {
  depends_on = [null_resource.stop_instance]

  provisioner "local-exec" {
    command = "gcloud compute instances set-machine-type ${var.instance_name} --zone=${var.zone} --project=${var.project_id} --machine-type=${var.new_machine_type}"
  }
}

# Start instance after resize
resource "null_resource" "start_instance" {
  depends_on = [null_resource.resize_instance]

  provisioner "local-exec" {
    command = "gcloud compute instances start ${var.instance_name} --zone=${var.zone} --project=${var.project_id}"
  }
}

# Wait for instance to be running
resource "null_resource" "wait_for_instance" {
  depends_on = [null_resource.start_instance]

  provisioner "local-exec" {
    command = <<-EOT
      for i in {1..30}; do
        STATUS=$(gcloud compute instances describe ${var.instance_name} --zone=${var.zone} --project=${var.project_id} --format='get(status)')
        if [ "$STATUS" = "RUNNING" ]; then
          echo "Instance is running"
          exit 0
        fi
        echo "Waiting for instance to start... ($i/30)"
        sleep 10
      done
      echo "Timeout waiting for instance"
      exit 1
    EOT
  }
}

output "instance_status" {
  value = "Instance ${var.instance_name} scaled to ${var.new_machine_type}"
}

output "previous_machine_type" {
  value = data.google_compute_instance.target.machine_type
}
