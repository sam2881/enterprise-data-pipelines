"""
Data Pipeline Consumer - Executes Data Pipeline Agent Workflows

WHY: Consumes pipeline.requested events from Kafka and triggers the
Data Pipeline Agent LangGraph workflow.

ARCHITECTURE (per user spec):
- Jira → Data Agent ONLY (no IT Service routing)
- Final output: GitHub Merge Request
- CI/CD validates and deploys to Airflow

FLOW:
1. pipeline.requested (Kafka) → This Consumer
2. Data Pipeline Agent → Generate Spark/DAG/DQ code
3. Create GitHub Merge Request with all generated code
4. CI/CD pipeline validates (code quality, security scans, DAG validation)
5. After MR approval → Deploy to Airflow
6. Airflow executes the pipeline (no AI logic here)

KAFKA TOPICS:
- pipeline.requested: Trigger pipeline generation
- pipeline.progress: Status updates
- pipeline.completed: Final result with MR URL
- pipeline.mr.created: GitHub MR created event
"""
import os
import sys
import json
import asyncio
from typing import Dict, Any
from datetime import datetime
import structlog
from kafka import KafkaConsumer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from streaming.kafka_producer import unified_producer
from agents.data.agent import data_pipeline_agent, PipelineTaskType

logger = structlog.get_logger()


# Kafka Topics
PIPELINE_REQUESTED_TOPIC = "pipeline.requested"
PIPELINE_PROGRESS_TOPIC = "pipeline.progress"
PIPELINE_COMPLETED_TOPIC = "pipeline.completed"
PIPELINE_MR_CREATED_TOPIC = "pipeline.mr.created"

# GitHub Configuration
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN', '')
GITHUB_OWNER = os.getenv('GITHUB_OWNER', '')
GITHUB_DATA_REPO = os.getenv('GITHUB_DATA_REPO', 'data-pipelines')
GITHUB_BASE_BRANCH = os.getenv('GITHUB_BASE_BRANCH', 'main')


class DataPipelineConsumer:
    """
    Consumes pipeline requests and executes Data Pipeline Agent.

    WHY: Event-driven architecture ensures reliable processing of
    pipeline requests from multiple sources (Jira, API, scheduled).

    Similar to how incident_consumer triggers IT Service Agent.
    """

    def __init__(self):
        self.bootstrap_servers = os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS',
            'localhost:9092'
        ).replace('kafka://', '')

        self.consumer_group = os.getenv(
            'PIPELINE_CONSUMER_GROUP',
            'data-pipeline-consumer'
        )

        self.consumer = None
        self._running = False

        logger.info(
            "data_pipeline_consumer_init",
            bootstrap_servers=self.bootstrap_servers,
            consumer_group=self.consumer_group
        )

    def connect(self):
        """Connect to Kafka"""
        try:
            self.consumer = KafkaConsumer(
                PIPELINE_REQUESTED_TOPIC,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.consumer_group,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=False,  # Manual commit after processing
                session_timeout_ms=60000,  # Longer timeout for pipeline processing
                heartbeat_interval_ms=20000,
            )
            logger.info("data_pipeline_consumer_connected")
            return True
        except Exception as e:
            logger.error("data_pipeline_consumer_connect_failed", error=str(e))
            return False

    async def start(self):
        """Start consuming pipeline requests"""
        if not self.connect():
            logger.error("Failed to connect to Kafka")
            return

        self._running = True
        logger.info("data_pipeline_consumer_started", topic=PIPELINE_REQUESTED_TOPIC)

        try:
            while self._running:
                # Poll for messages
                messages = self.consumer.poll(timeout_ms=1000)

                for topic_partition, records in messages.items():
                    for record in records:
                        await self._process_pipeline_request(record)
                        # Commit after successful processing
                        self.consumer.commit()

                await asyncio.sleep(0.1)

        except Exception as e:
            logger.error("data_pipeline_consumer_error", error=str(e))
        finally:
            self.stop()

    def stop(self):
        """Stop the consumer"""
        self._running = False
        if self.consumer:
            self.consumer.close()
            logger.info("data_pipeline_consumer_stopped")

    async def _process_pipeline_request(self, record):
        """Process a pipeline request"""
        event = record.value
        correlation_id = event.get("correlation_id", "unknown")
        request = event.get("request", {})

        logger.info(
            "pipeline_request_received",
            correlation_id=correlation_id,
            source_uri=request.get("source_uri"),
            jira_key=request.get("jira_key")
        )

        try:
            # Publish progress: Starting
            await self._publish_progress(correlation_id, "starting", request)

            # Execute Data Pipeline Agent
            result = await self._execute_pipeline_workflow(request, correlation_id)

            # Create GitHub Merge Request with generated code
            if result.get("status") == "success":
                await self._publish_progress(correlation_id, "creating_github_mr", request)
                mr_result = await self._create_github_mr(request, result, correlation_id)
                result["github_mr"] = mr_result

            # Publish completion
            await self._publish_completion(correlation_id, result, request)

            # Update Jira if applicable
            if request.get("jira_key"):
                await self._update_jira_ticket(request.get("jira_key"), result)

            logger.info(
                "pipeline_request_completed",
                correlation_id=correlation_id,
                status=result.get("status"),
                risk_score=result.get("result", {}).get("risk_score", 0)
            )

        except Exception as e:
            logger.error(
                "pipeline_request_failed",
                correlation_id=correlation_id,
                error=str(e)
            )

            # Publish failure
            await self._publish_completion(
                correlation_id,
                {"status": "error", "error": str(e)},
                request
            )

    async def _execute_pipeline_workflow(
        self,
        request: Dict[str, Any],
        correlation_id: str
    ) -> Dict[str, Any]:
        """Execute the Data Pipeline Agent workflow"""

        # Build task for the agent
        task = {
            "type": PipelineTaskType.FULL_PIPELINE,
            "request_id": correlation_id,
            "source_uri": request.get("source_uri", ""),
            "source_type": request.get("source_type", "gcs"),
            "target_layer": request.get("target_layer", "silver"),
            "business_context": request.get("business_context", ""),
            "schedule": request.get("schedule", "@daily"),
        }

        # Publish progress: Analyzing source
        await self._publish_progress(correlation_id, "analyzing_source", request)

        # Execute the agent
        result = await data_pipeline_agent.handle_task(task)

        return result

    async def _publish_progress(
        self,
        correlation_id: str,
        stage: str,
        request: Dict[str, Any]
    ):
        """Publish pipeline progress event"""
        event = {
            "correlation_id": correlation_id,
            "stage": stage,
            "timestamp": datetime.utcnow().isoformat(),
            "jira_key": request.get("jira_key"),
            "source_uri": request.get("source_uri"),
        }

        await unified_producer.publish_event(
            PIPELINE_PROGRESS_TOPIC,
            event,
            key=correlation_id
        )

    async def _publish_completion(
        self,
        correlation_id: str,
        result: Dict[str, Any],
        request: Dict[str, Any]
    ):
        """Publish pipeline completion event"""
        event = {
            "correlation_id": correlation_id,
            "status": result.get("status", "completed"),
            "timestamp": datetime.utcnow().isoformat(),
            "jira_key": request.get("jira_key"),
            "source_uri": request.get("source_uri"),
            "result": result.get("result", {}),
            "error": result.get("error"),
        }

        await unified_producer.publish_event(
            PIPELINE_COMPLETED_TOPIC,
            event,
            key=correlation_id
        )

    async def _create_github_mr(
        self,
        request: Dict[str, Any],
        result: Dict[str, Any],
        correlation_id: str
    ) -> Dict[str, Any]:
        """
        Create GitHub Merge Request with generated pipeline code.

        This is the FINAL OUTPUT of the Data Agent workflow.
        After MR is approved, CI/CD deploys to Airflow.

        Files created in MR:
        - pipelines/{pipeline_name}/spark_job.py
        - pipelines/{pipeline_name}/dag.py
        - pipelines/{pipeline_name}/dq_expectations.json
        - pipelines/{pipeline_name}/config.yaml
        """
        import httpx

        jira_key = request.get("jira_key", "unknown")
        pipeline_result = result.get("result", {})
        branch_name = f"pipeline/{jira_key.lower()}"
        pipeline_name = jira_key.lower().replace("-", "_")

        try:
            if not GITHUB_TOKEN:
                logger.warning("github_token_not_configured")
                return {"status": "skipped", "reason": "GitHub token not configured"}

            async with httpx.AsyncClient() as client:
                headers = {
                    "Authorization": f"Bearer {GITHUB_TOKEN}",
                    "Accept": "application/vnd.github.v3+json"
                }

                # 1. Get default branch SHA
                ref_response = await client.get(
                    f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_DATA_REPO}/git/ref/heads/{GITHUB_BASE_BRANCH}",
                    headers=headers
                )

                if ref_response.status_code != 200:
                    logger.error("github_get_ref_failed", status=ref_response.status_code)
                    return {"status": "error", "error": "Failed to get base branch"}

                base_sha = ref_response.json()["object"]["sha"]

                # 2. Create new branch
                branch_response = await client.post(
                    f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_DATA_REPO}/git/refs",
                    headers=headers,
                    json={
                        "ref": f"refs/heads/{branch_name}",
                        "sha": base_sha
                    }
                )

                if branch_response.status_code not in [201, 422]:  # 422 = already exists
                    logger.error("github_create_branch_failed", status=branch_response.status_code)

                # 3. Create files
                files_to_create = self._prepare_pipeline_files(pipeline_name, pipeline_result, request)

                for file_path, content in files_to_create.items():
                    await client.put(
                        f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_DATA_REPO}/contents/{file_path}",
                        headers=headers,
                        json={
                            "message": f"[{jira_key}] Add {file_path.split('/')[-1]}",
                            "content": self._base64_encode(content),
                            "branch": branch_name
                        }
                    )

                # 4. Create Pull Request
                pr_body = self._build_pr_description(request, pipeline_result, correlation_id)

                pr_response = await client.post(
                    f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_DATA_REPO}/pulls",
                    headers=headers,
                    json={
                        "title": f"[{jira_key}] Data Pipeline: {request.get('jira_summary', pipeline_name)}",
                        "head": branch_name,
                        "base": GITHUB_BASE_BRANCH,
                        "body": pr_body
                    }
                )

                if pr_response.status_code == 201:
                    pr_data = pr_response.json()
                    mr_url = pr_data["html_url"]
                    mr_number = pr_data["number"]

                    # Publish MR created event
                    await unified_producer.publish_event(
                        PIPELINE_MR_CREATED_TOPIC,
                        {
                            "correlation_id": correlation_id,
                            "jira_key": jira_key,
                            "mr_url": mr_url,
                            "mr_number": mr_number,
                            "branch": branch_name,
                            "timestamp": datetime.utcnow().isoformat()
                        },
                        key=correlation_id
                    )

                    logger.info(
                        "github_mr_created",
                        jira_key=jira_key,
                        mr_url=mr_url,
                        mr_number=mr_number
                    )

                    return {
                        "status": "created",
                        "mr_url": mr_url,
                        "mr_number": mr_number,
                        "branch": branch_name
                    }
                else:
                    logger.error("github_create_pr_failed", status=pr_response.status_code)
                    return {"status": "error", "error": pr_response.text}

        except Exception as e:
            logger.error("github_mr_creation_failed", error=str(e))
            return {"status": "error", "error": str(e)}

    def _prepare_pipeline_files(
        self,
        pipeline_name: str,
        result: Dict[str, Any],
        request: Dict[str, Any]
    ) -> Dict[str, str]:
        """Prepare files to be committed to GitHub"""
        base_path = f"pipelines/{pipeline_name}"

        files = {}

        # Spark job
        spark_code = result.get("spark", {}).get("code", "")
        if spark_code:
            files[f"{base_path}/spark_job.py"] = spark_code

        # Airflow DAG
        dag_code = result.get("dag", {}).get("code", "")
        if dag_code:
            files[f"{base_path}/dag.py"] = dag_code

        # Data Quality expectations
        dq_suite = result.get("dq", {}).get("suite", {})
        if dq_suite:
            files[f"{base_path}/dq_expectations.json"] = json.dumps(dq_suite, indent=2)

        # Pipeline config
        config = {
            "pipeline_name": pipeline_name,
            "jira_key": request.get("jira_key"),
            "source_uri": request.get("source_uri"),
            "source_type": request.get("source_type"),
            "target_layer": request.get("target_layer"),
            "schedule": request.get("schedule", "@daily"),
            "created_by": request.get("requested_by"),
            "business_context": request.get("business_context"),
            "generated_at": datetime.utcnow().isoformat()
        }
        files[f"{base_path}/config.yaml"] = self._dict_to_yaml(config)

        # IR (Intermediate Representation) for debugging
        ir = result.get("ir", {})
        if ir:
            files[f"{base_path}/ir.json"] = json.dumps(ir, indent=2)

        return files

    def _base64_encode(self, content: str) -> str:
        """Base64 encode content for GitHub API"""
        import base64
        return base64.b64encode(content.encode()).decode()

    def _dict_to_yaml(self, data: Dict[str, Any]) -> str:
        """Convert dict to YAML-like format"""
        lines = []
        for key, value in data.items():
            if isinstance(value, str) and "\n" in value:
                lines.append(f"{key}: |")
                for line in value.split("\n"):
                    lines.append(f"  {line}")
            else:
                lines.append(f"{key}: {value}")
        return "\n".join(lines)

    def _build_pr_description(
        self,
        request: Dict[str, Any],
        result: Dict[str, Any],
        correlation_id: str
    ) -> str:
        """Build PR description with all pipeline details"""
        risk_score = result.get("risk_score", 0)
        ir = result.get("ir", {})

        return f"""## Data Pipeline Generation

**Jira Ticket:** [{request.get('jira_key')}](https://jira.example.com/browse/{request.get('jira_key')})
**Correlation ID:** `{correlation_id}`

### Pipeline Summary

| Property | Value |
|----------|-------|
| Source | `{request.get('source_uri', 'N/A')}` |
| Source Type | {request.get('source_type', 'gcs')} |
| Target Layer | {request.get('target_layer', 'silver')} |
| Schedule | {request.get('schedule', '@daily')} |
| Risk Score | {risk_score:.1%} |
| Requested By | {request.get('requested_by', 'Unknown')} |

### Business Context

{request.get('business_context', 'No context provided')}

### Generated Artifacts

- [x] Spark transformation code (`spark_job.py`)
- [x] Airflow DAG (`dag.py`)
- [x] Data Quality expectations (`dq_expectations.json`)
- [x] Pipeline config (`config.yaml`)

### CI/CD Checklist

After this MR is approved:
1. CI validates code quality and security
2. DAG validation runs
3. Pipeline is deployed to Airflow
4. No AI logic in Airflow - clean execution only

---
*Generated by Data Pipeline Agent*
*Correlation ID: {correlation_id}*
"""

    async def _update_jira_ticket(self, jira_key: str, result: Dict[str, Any]):
        """Update Jira ticket with pipeline results"""
        try:
            # Import Jira client
            from agents.jira.jira_client import JiraClient

            jira = JiraClient()

            status = result.get("status", "unknown")
            pipeline_result = result.get("result", {})

            if status == "success":
                # Build success comment
                comment = self._build_success_comment(pipeline_result)
                await jira.add_comment(jira_key, comment)

                # Transition to "Done" or "In Review" based on risk
                if pipeline_result.get("requires_approval", False):
                    await jira.transition_issue(jira_key, "In Review")
                else:
                    await jira.transition_issue(jira_key, "Done")

            else:
                # Build error comment
                comment = f"""
*Data Pipeline Agent - Failed*

Error: {result.get('error', 'Unknown error')}

Please review and retry.
"""
                await jira.add_comment(jira_key, comment)

            logger.info("jira_updated", jira_key=jira_key, status=status)

        except Exception as e:
            logger.error("jira_update_failed", jira_key=jira_key, error=str(e))

    def _build_success_comment(self, result: Dict[str, Any]) -> str:
        """Build Jira comment for successful pipeline"""
        risk_score = result.get("risk_score", 0)
        requires_approval = result.get("requires_approval", False)
        ir = result.get("ir", {})
        spark = result.get("spark", {})
        dag = result.get("dag", {})
        dq = result.get("dq", {})

        approval_note = ""
        if requires_approval:
            approval_note = """
*APPROVAL REQUIRED*
Risk Score: {:.1%} (threshold: 70%)
Please review the generated pipeline before deployment.
""".format(risk_score)

        return f"""
*Data Pipeline Agent - Pipeline Generated Successfully*

{approval_note}

*Summary:*
- Source: {ir.get('source', {}).get('uri', 'N/A')}
- Target Layer: {ir.get('metadata', {}).get('target_layer', 'N/A')}
- Schedule: {dag.get('schedule', '@daily')}
- Risk Score: {risk_score:.1%}

*Generated Artifacts:*
- Spark Code: {len(spark.get('code', ''))} characters
- Airflow DAG: {len(dag.get('code', ''))} characters
- DQ Expectations: {len(dq.get('suite', {}).get('expectations', []))} rules

*Transformations:*
{self._format_transformations(ir.get('transformations', []))}

*Next Steps:*
1. Review generated code in the platform
2. Approve for deployment (if required)
3. DAG will be deployed to Airflow automatically

---
_Generated by Data Pipeline Agent via LangGraph workflow_
"""

    def _format_transformations(self, transformations: list) -> str:
        """Format transformations for Jira comment"""
        if not transformations:
            return "- No transformations defined"

        lines = []
        for t in transformations[:5]:  # Limit to 5
            lines.append(f"- {t.get('type', 'unknown')}")

        if len(transformations) > 5:
            lines.append(f"- ... and {len(transformations) - 5} more")

        return "\n".join(lines)


# Singleton consumer instance
data_pipeline_consumer = DataPipelineConsumer()


async def main():
    """Run the Data Pipeline Consumer"""
    print("Starting Data Pipeline Consumer...")
    print("Listening for pipeline.requested events")
    print("Press Ctrl+C to stop")

    try:
        await data_pipeline_consumer.start()
    except KeyboardInterrupt:
        data_pipeline_consumer.stop()
        print("\nConsumer stopped")


if __name__ == "__main__":
    asyncio.run(main())
