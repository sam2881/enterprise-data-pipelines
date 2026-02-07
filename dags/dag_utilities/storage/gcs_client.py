"""
APEX Data Agent - GCS Client

Google Cloud Storage client for APEX operations.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime


class GCSClient:
    """
    Google Cloud Storage client.

    Provides GCS-specific operations beyond FileOperations.
    """

    def __init__(self, project: Optional[str] = None):
        """Initialize GCS client."""
        import os
        self.project = project or os.getenv("GCP_PROJECT")
        self._client = None

    @property
    def client(self):
        """Get GCS client (lazy initialization)."""
        if self._client is None:
            from google.cloud import storage
            self._client = storage.Client(project=self.project)
        return self._client

    def get_bucket(self, bucket_name: str):
        """Get a GCS bucket."""
        return self.client.bucket(bucket_name)

    def create_signed_url(
        self,
        bucket_name: str,
        blob_name: str,
        expiration_minutes: int = 60,
    ) -> str:
        """Create a signed URL for a blob."""
        from datetime import timedelta
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.generate_signed_url(
            expiration=timedelta(minutes=expiration_minutes),
            method="GET",
        )

    def compose_files(
        self,
        bucket_name: str,
        source_blobs: List[str],
        destination_blob: str,
    ) -> bool:
        """Compose multiple blobs into one."""
        bucket = self.client.bucket(bucket_name)
        sources = [bucket.blob(name) for name in source_blobs]
        destination = bucket.blob(destination_blob)
        destination.compose(sources)
        return True

    def set_metadata(
        self,
        bucket_name: str,
        blob_name: str,
        metadata: Dict[str, str],
    ) -> None:
        """Set custom metadata on a blob."""
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.metadata = metadata
        blob.patch()

    def get_metadata(
        self,
        bucket_name: str,
        blob_name: str,
    ) -> Dict[str, Any]:
        """Get blob metadata."""
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.reload()
        return {
            "name": blob.name,
            "size": blob.size,
            "updated": blob.updated,
            "md5_hash": blob.md5_hash,
            "content_type": blob.content_type,
            "metadata": blob.metadata,
        }
