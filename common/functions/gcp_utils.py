"""GCP Utility Functions"""
from google.cloud import storage, bigquery

PROJECT_ID = "agent-ai-test-461120"
BUCKETS = {"landing": f"{PROJECT_ID}-raw-data", "bronze": f"{PROJECT_ID}-bronze", "silver": f"{PROJECT_ID}-silver", "gold": f"{PROJECT_ID}-gold"}

def get_gcs_client(): return storage.Client()
def get_bq_client(): return bigquery.Client()
def list_files(bucket, prefix, suffix=""): return [f"gs://{bucket}/{b.name}" for b in get_gcs_client().list_blobs(bucket, prefix=prefix) if b.name.endswith(suffix)]
