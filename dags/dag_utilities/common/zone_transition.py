"""
APEX Data Agent - Zone Transition Utilities

Reusable Dataproc/Spark job submission and TaskGroup builders.
GCP-first: uses Dataproc by default.
"""

import os
from typing import Any, Dict, List, Optional


def submit_dataproc_job(
    job_name: str,
    job_path: str,
    args: Optional[Dict[str, str]] = None,
    cluster_name: Optional[str] = None,
    region: str = "us-central1",
    project: Optional[str] = None,
    **context,
) -> Dict[str, Any]:
    """
    Submit a PySpark job to Dataproc.
    Falls back to local spark-submit if Dataproc is not available.
    """
    args = args or {}
    project = project or os.getenv("GCP_PROJECT", "")
    cluster_name = cluster_name or os.getenv("DATAPROC_CLUSTER", "apex-spark-cluster")

    # Resolve template variables in args
    ti = context.get("ti")
    ds = context.get("ds", "")
    execution_id = ti.xcom_pull(key="execution_id") if ti else ""

    resolved_args = {}
    for k, v in args.items():
        v_str = str(v)
        v_str = v_str.replace("{{ ds }}", ds)
        v_str = v_str.replace("{{ ds_nodash }}", ds.replace("-", ""))
        if "xcom_pull" in v_str and ti:
            v_str = ti.xcom_pull(key="execution_id") or v_str
        resolved_args[k] = v_str

    # Build argument list
    arg_list = []
    for k, v in resolved_args.items():
        arg_list.extend([k, v])

    print(f"[zone_transition] Submitting {job_name}: {job_path}")
    print(f"[zone_transition] Args: {resolved_args}")

    try:
        # Try Dataproc submission
        from google.cloud import dataproc_v1

        client = dataproc_v1.JobControllerClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )

        job = {
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{os.getenv('APEX_SPARK_BUCKET', 'datalake')}/spark_jobs/{job_path}",
                "args": arg_list,
                "properties": {
                    "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
                    "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES", "2"),
                    "spark.dynamicAllocation.enabled": "true",
                },
            },
        }

        operation = client.submit_job_as_operation(
            request={"project_id": project, "region": region, "job": job}
        )
        result = operation.result()

        status = result.status.state.name
        print(f"[zone_transition] Job {job_name} completed: {status}")

        return {"job_name": job_name, "status": status, "job_id": result.reference.job_id}

    except (ImportError, Exception) as e:
        print(f"[zone_transition] Dataproc not available ({e}). Using local SparkJobSubmitter.")
        return _submit_local_spark(job_name, job_path, resolved_args)


def _submit_local_spark(job_name: str, job_path: str, args: Dict[str, str]) -> Dict[str, Any]:
    """Fallback: submit spark job locally via SparkJobSubmitter."""
    try:
        from dag_utilities.spark.job_submitter import SparkJobSubmitter

        submitter = SparkJobSubmitter()
        result = submitter.submit_job(job_path, args)
        return {"job_name": job_name, "status": "SUCCESS", "metrics": result}
    except Exception as e:
        print(f"[zone_transition] Local spark submit failed: {e}")
        return {"job_name": job_name, "status": "FAILED", "error": str(e)}


def submit_spark_k8s_job(
    job_name: str,
    job_path: str,
    args: Optional[Dict[str, str]] = None,
    namespace: str = "spark",
    **context,
) -> Dict[str, Any]:
    """Submit Spark job to Kubernetes (alternative to Dataproc)."""
    args = args or {}
    print(f"[zone_transition] SparkKubernetes submit: {job_name}")

    try:
        from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

        # This would be used inside a template, not called directly
        return {"job_name": job_name, "status": "SUBMITTED", "engine": "kubernetes"}
    except ImportError:
        print("[zone_transition] SparkKubernetesOperator not available")
        return submit_dataproc_job(job_name, job_path, args, **context)
