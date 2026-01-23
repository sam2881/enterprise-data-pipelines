#!/usr/bin/env python3
"""
Airflow DAG Failure Monitor - Monitors DAG runs and creates ServiceNow incidents

This implements automated incident creation when Airflow DAGs fail:
- PHASE 1: Detects DAG failures via Airflow REST API
- PHASE 2: Creates ServiceNow incident with full context
- PHASE 3: Publishes to Kafka for agent processing
- PHASE 4: Agent can auto-remediate by retriggering via Airflow MCP

Architecture:
  Airflow (failed DAG) → This Monitor → ServiceNow Incident → Kafka Event
                                                              ↓
                                        LangGraph Workflow → Airflow MCP → Retrigger DAG
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, timedelta

# Force unbuffered output for logging
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

from requests.auth import HTTPBasicAuth
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
AIRFLOW_URL = os.getenv('AIRFLOW_URL', 'http://localhost:8083')
AIRFLOW_USERNAME = os.getenv('AIRFLOW_USERNAME', 'admin')
AIRFLOW_PASSWORD = os.getenv('AIRFLOW_PASSWORD', 'admin')

SNOW_URL = os.getenv('SNOW_INSTANCE_URL', 'https://dev349960.service-now.com')
SNOW_USER = os.getenv('SNOW_USERNAME', 'admin')
SNOW_PASS = os.getenv('SNOW_PASSWORD', '')

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
POLL_INTERVAL = int(os.getenv('AIRFLOW_POLL_INTERVAL', '60'))  # seconds
LOOKBACK_MINUTES = int(os.getenv('AIRFLOW_LOOKBACK_MINUTES', '5'))  # Check last N minutes

# Track DAG runs we've already created incidents for (to avoid duplicates)
processed_dag_runs = {}


class AirflowDAGMonitor:
    """Monitors Airflow DAG runs and creates ServiceNow incidents for failures"""

    def __init__(self):
        print("🔧 Initializing Airflow DAG Monitor...")
        print(f"   Airflow URL: {AIRFLOW_URL}")
        print(f"   ServiceNow: {SNOW_URL}")
        print(f"   Kafka: {KAFKA_BOOTSTRAP}")
        print(f"   Lookback: {LOOKBACK_MINUTES} minutes")

        # Airflow auth
        self.airflow_auth = HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)

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

        print("✅ Airflow DAG Monitor initialized!\n")

    def get_failed_dag_runs(self):
        """Get failed DAG runs from Airflow API"""
        failed_runs = []

        # Calculate time window
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=LOOKBACK_MINUTES)

        try:
            # Get all DAG runs with failed state
            # Airflow 2.x REST API endpoint
            params = {
                'state': 'failed',
                'start_date_gte': start_time.isoformat() + 'Z',
                'end_date_lte': end_time.isoformat() + 'Z',
                'limit': 100
            }

            response = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/~/dagRuns",
                auth=self.airflow_auth,
                params=params,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                dag_runs = data.get('dag_runs', [])

                for run in dag_runs:
                    run_key = f"{run['dag_id']}-{run['dag_run_id']}"

                    # Skip if already processed
                    if run_key in processed_dag_runs:
                        continue

                    failed_runs.append({
                        'dag_id': run['dag_id'],
                        'dag_run_id': run['dag_run_id'],
                        'execution_date': run.get('execution_date', ''),
                        'start_date': run.get('start_date', ''),
                        'end_date': run.get('end_date', ''),
                        'state': run['state'],
                        'external_trigger': run.get('external_trigger', False),
                        'conf': run.get('conf', {})
                    })

            elif response.status_code == 401:
                print("❌ Airflow authentication failed")
            else:
                print(f"⚠️ Airflow API error: {response.status_code}")

        except requests.exceptions.ConnectionError:
            print(f"⚠️ Cannot connect to Airflow at {AIRFLOW_URL}")
        except Exception as e:
            print(f"⚠️ Error fetching DAG runs: {e}")

        return failed_runs

    def get_failed_tasks(self, dag_id, dag_run_id):
        """Get failed tasks for a specific DAG run"""
        failed_tasks = []

        try:
            response = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
                auth=self.airflow_auth,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                task_instances = data.get('task_instances', [])

                for task in task_instances:
                    if task['state'] == 'failed':
                        failed_tasks.append({
                            'task_id': task['task_id'],
                            'state': task['state'],
                            'start_date': task.get('start_date', ''),
                            'end_date': task.get('end_date', ''),
                            'try_number': task.get('try_number', 1),
                            'max_tries': task.get('max_tries', 1)
                        })

        except Exception as e:
            print(f"⚠️ Error fetching task instances: {e}")

        return failed_tasks

    def get_task_logs(self, dag_id, dag_run_id, task_id, try_number=1):
        """Get logs for a specific task"""
        try:
            response = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}",
                auth=self.airflow_auth,
                headers={'Accept': 'text/plain'},
                timeout=30
            )

            if response.status_code == 200:
                # Get last 50 lines of logs
                logs = response.text
                log_lines = logs.split('\n')
                return '\n'.join(log_lines[-50:])

        except Exception as e:
            print(f"⚠️ Error fetching task logs: {e}")

        return "Logs not available"

    def check_for_failures(self):
        """Check for failed DAG runs"""
        print(f"🔍 Scanning Airflow at {datetime.now().strftime('%H:%M:%S')}...")

        failed_runs = self.get_failed_dag_runs()

        if failed_runs:
            for run in failed_runs:
                print(f"   🚨 Found failed DAG: {run['dag_id']} (run: {run['dag_run_id']})")

        return failed_runs

    def create_servicenow_incident(self, dag_run, failed_tasks, task_logs):
        """Create ServiceNow incident for failed DAG"""
        try:
            # Build task failure details
            task_details = ""
            for task in failed_tasks:
                task_details += f"\n- Task: {task['task_id']}"
                task_details += f"\n  Try: {task['try_number']}/{task['max_tries']}"
                task_details += f"\n  Start: {task['start_date']}"
                task_details += f"\n  End: {task['end_date']}"

            incident_data = {
                'short_description': f"[Airflow Alert] DAG '{dag_run['dag_id']}' failed - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                'description': f"""Airflow DAG Failure Alert

DAG Information:
- DAG ID: {dag_run['dag_id']}
- Run ID: {dag_run['dag_run_id']}
- Execution Date: {dag_run['execution_date']}
- Start Time: {dag_run['start_date']}
- End Time: {dag_run['end_date']}
- State: {dag_run['state']}
- External Trigger: {dag_run['external_trigger']}

Failed Tasks:{task_details}

Task Logs (Last 50 lines):
{task_logs[:2000]}

Recommended Actions:
1. Review task logs in Airflow UI
2. Check data source availability
3. Verify task dependencies
4. Consider retrying the DAG run

Airflow Dashboard: {AIRFLOW_URL}/dags/{dag_run['dag_id']}/grid

Note: This incident can be auto-remediated by the AI Agent system.
The agent can analyze the failure and retrigger the DAG if appropriate.
""",
                'priority': '2',  # High
                'urgency': '2',
                'impact': '2',
                'category': 'Data Pipeline',
                'subcategory': 'Airflow',
                'assignment_group': 'Data Engineering'
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

    def publish_to_kafka(self, dag_run, failed_tasks, incident_result):
        """Publish DAG failure event to Kafka for agent processing"""
        if not self.kafka_producer:
            return

        try:
            event = {
                'event_type': 'airflow_dag_failure',
                'timestamp': datetime.now().isoformat(),
                'source': 'airflow_dag_monitor',
                'dag_failure': {
                    'dag_id': dag_run['dag_id'],
                    'dag_run_id': dag_run['dag_run_id'],
                    'execution_date': dag_run['execution_date'],
                    'start_date': dag_run['start_date'],
                    'end_date': dag_run['end_date'],
                    'state': dag_run['state'],
                    'failed_tasks': failed_tasks,
                    'airflow_url': AIRFLOW_URL
                },
                'incident': incident_result,
                'remediation_hints': {
                    'can_retry': True,
                    'retry_endpoint': f"{AIRFLOW_URL}/api/v1/dags/{dag_run['dag_id']}/dagRuns",
                    'clear_task_endpoint': f"{AIRFLOW_URL}/api/v1/dags/{dag_run['dag_id']}/clearTaskInstances",
                    'mcp_tool': 'airflow_mcp.trigger_dag'
                }
            }

            # Publish to airflow.failures topic
            future = self.kafka_producer.send(
                'airflow.failures',
                key=dag_run['dag_id'],
                value=event
            )
            record_metadata = future.get(timeout=10)
            print(f"   📤 Published to Kafka: airflow.failures (partition: {record_metadata.partition})")

            # Also publish to incident.created for the main workflow
            incident_event = {
                'event_type': 'incident.created',
                'timestamp': datetime.now().isoformat(),
                'source': 'airflow_dag_monitor',
                'incident': {
                    'number': incident_result.get('incident_id', 'UNKNOWN'),
                    'sys_id': incident_result.get('sys_id', ''),
                    'short_description': f"DAG '{dag_run['dag_id']}' failed",
                    'category': 'Data Pipeline',
                    'subcategory': 'Airflow',
                    'priority': '2',
                    'state': 'new'
                },
                'context': {
                    'dag_id': dag_run['dag_id'],
                    'dag_run_id': dag_run['dag_run_id'],
                    'failed_tasks': [t['task_id'] for t in failed_tasks],
                    'remediation_available': True
                }
            }

            self.kafka_producer.send(
                'incident.created',
                key=incident_result.get('incident_id', dag_run['dag_id']),
                value=incident_event
            )
            print(f"   📤 Published to Kafka: incident.created")

        except Exception as e:
            print(f"   ⚠️ Kafka publish error: {e}")

    def process_failed_runs(self, failed_runs):
        """Process all failed DAG runs - create incidents and publish events"""
        for dag_run in failed_runs:
            run_key = f"{dag_run['dag_id']}-{dag_run['dag_run_id']}"

            print(f"\n📋 Processing: {dag_run['dag_id']} (run: {dag_run['dag_run_id']})")

            # Get failed tasks
            failed_tasks = self.get_failed_tasks(dag_run['dag_id'], dag_run['dag_run_id'])
            print(f"   Found {len(failed_tasks)} failed task(s)")

            # Get logs from first failed task
            task_logs = "No failed tasks found"
            if failed_tasks:
                first_task = failed_tasks[0]
                task_logs = self.get_task_logs(
                    dag_run['dag_id'],
                    dag_run['dag_run_id'],
                    first_task['task_id'],
                    first_task.get('try_number', 1)
                )

            # Create ServiceNow incident
            incident_result = self.create_servicenow_incident(dag_run, failed_tasks, task_logs)

            # Publish to Kafka
            self.publish_to_kafka(dag_run, failed_tasks, incident_result)

            # Mark as processed
            processed_dag_runs[run_key] = datetime.now()

        if failed_runs:
            print(f"\n✅ Processed {len(failed_runs)} failed DAG run(s)")

    def run(self):
        """Main monitoring loop"""
        print(f"🚀 Starting Airflow DAG Monitor...")
        print(f"   Poll interval: {POLL_INTERVAL}s")
        print(f"   Press Ctrl+C to stop\n")

        try:
            while True:
                # Check for failed DAG runs
                failed_runs = self.check_for_failures()

                # Process any failures found
                if failed_runs:
                    self.process_failed_runs(failed_runs)
                else:
                    print(f"   ✅ No new DAG failures detected")

                # Sleep until next poll
                print(f"   😴 Next check in {POLL_INTERVAL}s...\n")
                time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            print("\n⏹️ Shutting down Airflow DAG Monitor...")
            if self.kafka_producer:
                self.kafka_producer.close()
            print("✅ Monitor stopped")

    def check_once(self):
        """Run a single check and exit"""
        print("🔍 Running single Airflow check...\n")

        failed_runs = self.check_for_failures()

        if failed_runs:
            self.process_failed_runs(failed_runs)
        else:
            print("✅ No failed DAG runs found")

        if self.kafka_producer:
            self.kafka_producer.close()

    def list_recent_runs(self):
        """List recent DAG runs"""
        print("📊 Listing recent DAG runs...\n")

        try:
            # Get DAG runs from last hour
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=1)

            response = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/~/dagRuns",
                auth=self.airflow_auth,
                params={
                    'start_date_gte': start_time.isoformat() + 'Z',
                    'limit': 50
                },
                headers={'Content-Type': 'application/json'},
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                dag_runs = data.get('dag_runs', [])

                success = [r for r in dag_runs if r['state'] == 'success']
                failed = [r for r in dag_runs if r['state'] == 'failed']
                running = [r for r in dag_runs if r['state'] == 'running']
                queued = [r for r in dag_runs if r['state'] == 'queued']

                print(f"✅ Successful runs ({len(success)}):")
                for run in success[:5]:
                    print(f"   - {run['dag_id']} ({run['dag_run_id']})")

                print(f"\n🚨 Failed runs ({len(failed)}):")
                for run in failed[:5]:
                    print(f"   - {run['dag_id']} ({run['dag_run_id']})")

                print(f"\n🔄 Running ({len(running)}):")
                for run in running[:5]:
                    print(f"   - {run['dag_id']} ({run['dag_run_id']})")

                print(f"\n⏳ Queued ({len(queued)}):")
                for run in queued[:5]:
                    print(f"   - {run['dag_id']} ({run['dag_run_id']})")

                print(f"\nTotal: {len(dag_runs)} runs in last hour")
            else:
                print(f"❌ Failed to fetch DAG runs: {response.status_code}")

        except requests.exceptions.ConnectionError:
            print(f"❌ Cannot connect to Airflow at {AIRFLOW_URL}")
        except Exception as e:
            print(f"❌ Error: {e}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Airflow DAG Monitor - Detects failures and creates incidents')
    parser.add_argument('--once', action='store_true', help='Run single check and exit')
    parser.add_argument('--list', action='store_true', help='List recent DAG runs and exit')
    parser.add_argument('--interval', type=int, default=60, help='Poll interval in seconds')
    parser.add_argument('--lookback', type=int, default=5, help='Lookback period in minutes')
    args = parser.parse_args()

    if args.interval:
        POLL_INTERVAL = args.interval
    if args.lookback:
        LOOKBACK_MINUTES = args.lookback

    monitor = AirflowDAGMonitor()

    if args.list:
        monitor.list_recent_runs()
    elif args.once:
        monitor.check_once()
    else:
        monitor.run()
