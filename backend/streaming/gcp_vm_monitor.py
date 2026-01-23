#!/usr/bin/env python3
"""
GCP VM Monitor - Continuous monitoring for stopped/terminated VMs
Automatically creates ServiceNow incidents and publishes to Kafka

This implements PHASE 1 & 2 of the lifecycle:
- PHASE 1: Detects failures (VM stopped/terminated)
- PHASE 2: GCP Monitoring triggers alerts

DEDUPLICATION: Before creating incidents, checks ServiceNow for existing
open incidents for the same VM to avoid duplicates.
"""

import os
import sys
import json
import time
import requests
from datetime import datetime

# Force unbuffered output for logging
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)
from requests.auth import HTTPBasicAuth
from google.cloud import compute_v1
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'agent-ai-test-461120')
GCP_ZONES = ['us-central1-a', 'us-central1-b', 'us-east1-b']  # Zones to monitor
GOOGLE_APPLICATION_CREDENTIALS = os.getenv(
    'GOOGLE_APPLICATION_CREDENTIALS',
    '/home/samrattidke600/ai_agent_app/gcp-service-account-key.json'
)

SNOW_URL = os.getenv('SNOW_INSTANCE_URL', 'https://dev349960.service-now.com')
SNOW_USER = os.getenv('SNOW_USERNAME', 'admin')
SNOW_PASS = os.getenv('SNOW_PASSWORD')

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
POLL_INTERVAL = int(os.getenv('GCP_POLL_INTERVAL', '60'))  # seconds

# Track VMs we've already created incidents for (in-memory cache)
# This is a fallback - primary deduplication is via ServiceNow query
processed_vms = {}


class GCPVMMonitor:
    """Monitors GCP VMs and creates ServiceNow incidents for stopped VMs"""

    def __init__(self):
        print("🔧 Initializing GCP VM Monitor...")
        print(f"   GCP Project: {GCP_PROJECT_ID}")
        print(f"   Zones: {', '.join(GCP_ZONES)}")
        print(f"   ServiceNow: {SNOW_URL}")
        print(f"   Kafka: {KAFKA_BOOTSTRAP}")

        # Set GCP credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS

        # Initialize GCP Compute client
        self.compute_client = compute_v1.InstancesClient()

        # Initialize Kafka producer
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3
            )
            print("✅ Kafka producer initialized")
        except Exception as e:
            print(f"⚠️ Kafka not available: {e}")
            self.kafka_producer = None

        # ServiceNow auth
        self.snow_auth = HTTPBasicAuth(SNOW_USER, SNOW_PASS)

        # Cache of existing open incidents (vm_name -> incident_number)
        self.existing_incidents = {}
        self._load_existing_incidents()

        print("✅ GCP VM Monitor initialized!\n")

    def _load_existing_incidents(self):
        """Load existing open GCP incidents from ServiceNow to avoid duplicates"""
        try:
            # Query for open incidents with GCP Alert in short_description
            # State 1=New, 2=In Progress, 3=On Hold - all are "open"
            response = requests.get(
                f"{SNOW_URL}/api/now/table/incident",
                auth=self.snow_auth,
                params={
                    'sysparm_query': 'short_descriptionLIKE[GCP Alert]^stateIN1,2,3',
                    'sysparm_fields': 'number,short_description,sys_id,state',
                    'sysparm_limit': 100
                },
                headers={'Accept': 'application/json'},
                timeout=30
            )

            if response.status_code == 200:
                incidents = response.json().get('result', [])
                for inc in incidents:
                    # Extract VM name from short_description
                    # Format: "[GCP Alert] VM instance {vm_name} is {status}"
                    desc = inc.get('short_description', '')
                    if 'VM instance' in desc:
                        try:
                            vm_name = desc.split('VM instance ')[1].split(' is ')[0]
                            self.existing_incidents[vm_name] = inc.get('number')
                        except IndexError:
                            pass

                print(f"   📋 Loaded {len(self.existing_incidents)} existing open GCP incidents")
                for vm, inc_num in self.existing_incidents.items():
                    print(f"      - {vm}: {inc_num}")
            else:
                print(f"   ⚠️ Could not load existing incidents: {response.status_code}")

        except Exception as e:
            print(f"   ⚠️ Error loading existing incidents: {e}")

    def has_open_incident(self, vm_name):
        """Check if VM already has an open incident"""
        return vm_name in self.existing_incidents

    def get_all_vms(self):
        """Get all VMs across configured zones"""
        all_vms = []

        for zone in GCP_ZONES:
            try:
                request = compute_v1.ListInstancesRequest(
                    project=GCP_PROJECT_ID,
                    zone=zone
                )
                instances = list(self.compute_client.list(request=request))

                for inst in instances:
                    all_vms.append({
                        'name': inst.name,
                        'zone': zone,
                        'status': inst.status,
                        'machine_type': inst.machine_type.split('/')[-1] if inst.machine_type else 'unknown',
                        'creation_timestamp': inst.creation_timestamp,
                        'id': inst.id
                    })

            except Exception as e:
                print(f"⚠️ Error fetching VMs from {zone}: {e}")

        return all_vms

    def check_for_stopped_vms(self):
        """Check all VMs and detect stopped/terminated ones"""
        print(f"🔍 Scanning VMs at {datetime.now().strftime('%H:%M:%S')}...")

        vms = self.get_all_vms()
        stopped_vms = []

        for vm in vms:
            status = vm['status']
            vm_key = f"{vm['name']}-{vm['zone']}"

            # Check if VM is stopped/terminated
            if status in ['TERMINATED', 'STOPPED', 'SUSPENDED']:
                # Check if we already processed this VM recently
                if vm_key in processed_vms:
                    last_processed = processed_vms[vm_key]
                    # Skip if processed within last 30 minutes
                    if (datetime.now() - last_processed).seconds < 1800:
                        continue

                stopped_vms.append(vm)
                print(f"   🚨 Found stopped VM: {vm['name']} ({status}) in {vm['zone']}")
            else:
                # VM is running - remove from processed if it was there
                if vm_key in processed_vms:
                    del processed_vms[vm_key]

        return stopped_vms

    def create_servicenow_incident(self, vm):
        """Create ServiceNow incident for stopped VM"""
        try:
            incident_data = {
                'short_description': f"[GCP Alert] VM instance {vm['name']} is {vm['status'].lower()} - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                'description': f"""GCP Monitoring Alert Triggered

Alert: VM Instance {vm['status']}
Instance: {vm['name']}
Zone: {vm['zone']}
Project: {GCP_PROJECT_ID}
Status: {vm['status']}
Machine Type: {vm['machine_type']}
Impact: Service may be unavailable

Alert Details:
- VM is not running
- Health checks may be failing
- Immediate attention required

Recommended Actions:
1. Check VM status in GCP Console
2. Review VM logs for errors
3. Start VM if appropriate
4. Verify application health after restart

Monitoring Dashboard: https://console.cloud.google.com/compute/instances?project={GCP_PROJECT_ID}
""",
                'priority': '1',  # Critical
                'urgency': '1',
                'impact': '1',
                'category': 'Infrastructure',
                'subcategory': 'Cloud',
                'assignment_group': 'Cloud Operations'
            }

            response = requests.post(
                f"{SNOW_URL}/api/now/table/incident",
                auth=self.snow_auth,
                json=incident_data,
                headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
                timeout=30
            )

            if response.status_code in [200, 201]:
                result = response.json().get('result', {})
                incident_id = result.get('number', 'Unknown')
                sys_id = result.get('sys_id', '')
                print(f"   ✅ ServiceNow incident created: {incident_id}")
                return {
                    'incident_id': incident_id,
                    'sys_id': sys_id,
                    'success': True
                }
            else:
                print(f"   ❌ Failed to create incident: {response.status_code} - {response.text[:200]}")
                return {'success': False, 'error': response.text}

        except Exception as e:
            print(f"   ❌ Error creating incident: {e}")
            return {'success': False, 'error': str(e)}

    def publish_to_kafka(self, vm, incident_result):
        """Publish alert event to Kafka"""
        if not self.kafka_producer:
            return

        try:
            event = {
                'event_type': 'gcp_vm_alert',
                'timestamp': datetime.now().isoformat(),
                'source': 'gcp_vm_monitor',
                'alert': {
                    'type': 'VM_STOPPED',
                    'vm_name': vm['name'],
                    'zone': vm['zone'],
                    'project': GCP_PROJECT_ID,
                    'status': vm['status'],
                    'machine_type': vm['machine_type']
                },
                'incident': incident_result
            }

            # Publish to gcp.alerts topic
            future = self.kafka_producer.send(
                'gcp.alerts',
                key=vm['name'],
                value=event
            )
            record_metadata = future.get(timeout=10)
            print(f"   📤 Published to Kafka: gcp.alerts (partition: {record_metadata.partition})")

        except Exception as e:
            print(f"   ⚠️ Kafka publish error: {e}")

    def process_stopped_vms(self, stopped_vms):
        """Process all stopped VMs - create incidents and publish events"""
        created_count = 0
        skipped_count = 0

        for vm in stopped_vms:
            vm_key = f"{vm['name']}-{vm['zone']}"
            vm_name = vm['name']

            print(f"\n📋 Processing: {vm_name} ({vm['status']})")

            # Check if there's already an open incident for this VM
            if self.has_open_incident(vm_name):
                existing_inc = self.existing_incidents[vm_name]
                print(f"   ⏭️ Skipping - Open incident already exists: {existing_inc}")
                skipped_count += 1

                # Still publish to Kafka for tracking (with existing incident ref)
                self.publish_to_kafka(vm, {
                    'incident_id': existing_inc,
                    'success': True,
                    'already_exists': True
                })
                continue

            # Create ServiceNow incident
            incident_result = self.create_servicenow_incident(vm)

            if incident_result.get('success'):
                # Track the new incident
                self.existing_incidents[vm_name] = incident_result.get('incident_id')
                created_count += 1

            # Publish to Kafka
            self.publish_to_kafka(vm, incident_result)

            # Mark as processed (in-memory fallback)
            processed_vms[vm_key] = datetime.now()

        if stopped_vms:
            print(f"\n" + "="*50)
            print(f"📊 Summary:")
            print(f"   ✅ Created: {created_count} new incident(s)")
            print(f"   ⏭️ Skipped: {skipped_count} (already have open incidents)")
            print("="*50)

    def run(self):
        """Main monitoring loop"""
        print(f"🚀 Starting GCP VM Monitor...")
        print(f"   Poll interval: {POLL_INTERVAL}s")
        print(f"   Press Ctrl+C to stop\n")

        try:
            while True:
                # Check for stopped VMs
                stopped_vms = self.check_for_stopped_vms()

                # Process any stopped VMs found
                if stopped_vms:
                    self.process_stopped_vms(stopped_vms)
                else:
                    print(f"   ✅ All VMs are running")

                # Sleep until next poll
                print(f"   😴 Next check in {POLL_INTERVAL}s...\n")
                time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            print("\n⏹️ Shutting down GCP VM Monitor...")
            if self.kafka_producer:
                self.kafka_producer.close()
            print("✅ Monitor stopped")

    def check_once(self):
        """Run a single check and exit"""
        print("🔍 Running single VM check...\n")

        stopped_vms = self.check_for_stopped_vms()

        if stopped_vms:
            self.process_stopped_vms(stopped_vms)
        else:
            print("✅ All VMs are running - no incidents needed")

        if self.kafka_producer:
            self.kafka_producer.close()

    def list_vms(self):
        """List all VMs and their status"""
        print("📊 Listing all VMs...\n")

        vms = self.get_all_vms()

        running = []
        stopped = []

        for vm in vms:
            if vm['status'] in ['RUNNING']:
                running.append(vm)
            else:
                stopped.append(vm)

        print(f"✅ Running VMs ({len(running)}):")
        for vm in running:
            print(f"   - {vm['name']} ({vm['zone']})")

        print(f"\n🚨 Stopped/Terminated VMs ({len(stopped)}):")
        for vm in stopped:
            print(f"   - {vm['name']} ({vm['zone']}) - {vm['status']}")

        print(f"\nTotal: {len(vms)} VMs")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='GCP VM Monitor - Detects stopped VMs and creates incidents')
    parser.add_argument('--once', action='store_true', help='Run single check and exit')
    parser.add_argument('--list', action='store_true', help='List all VMs and exit')
    parser.add_argument('--interval', type=int, default=60, help='Poll interval in seconds')
    args = parser.parse_args()

    if args.interval:
        POLL_INTERVAL = args.interval

    monitor = GCPVMMonitor()

    if args.list:
        monitor.list_vms()
    elif args.once:
        monitor.check_once()
    else:
        monitor.run()
