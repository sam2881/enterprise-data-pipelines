"""
APEX Data Agent - File Operations

High-level file operations for pipeline data movement.
Abstracts cloud storage (GCS, S3, ADLS) and local filesystem.
"""

import os
import re
import shutil
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

from dag_utilities.core.exceptions import StorageError


@dataclass
class FileInfo:
    """Information about a file."""

    path: str
    name: str
    size_bytes: int
    modified_time: datetime
    checksum: Optional[str] = None


class FileOperations:
    """
    High-level file operations for pipeline data.

    Provides abstraction over different storage backends:
    - GCS (gs://)
    - S3 (s3://)
    - ADLS (abfs://)
    - Local filesystem
    """

    def __init__(self):
        """Initialize file operations."""
        self._gcs_client = None
        self._s3_client = None

    def _get_gcs_client(self):
        """Get or create GCS client."""
        if self._gcs_client is None:
            from google.cloud import storage
            self._gcs_client = storage.Client()
        return self._gcs_client

    def _parse_gcs_path(self, path: str) -> tuple:
        """Parse GCS path into bucket and blob name."""
        if not path.startswith("gs://"):
            raise StorageError(f"Invalid GCS path: {path}")
        parts = path[5:].split("/", 1)
        bucket = parts[0]
        blob_name = parts[1] if len(parts) > 1 else ""
        return bucket, blob_name

    # ═══════════════════════════════════════════════════════════════════════
    # FILE CHECKING
    # ═══════════════════════════════════════════════════════════════════════

    def check_file_exists(
        self,
        path: str,
        pattern: Optional[str] = None,
    ) -> bool:
        """
        Check if file(s) exist at the given path.

        Args:
            path: Base path to check
            pattern: Optional regex pattern to match files

        Returns:
            True if matching files exist
        """
        files = self.list_files(path, pattern)
        return len(files) > 0

    def list_files(
        self,
        path: str,
        pattern: Optional[str] = None,
    ) -> List[FileInfo]:
        """
        List files at the given path.

        Args:
            path: Path to list (supports gs://, s3://, local)
            pattern: Optional regex pattern to filter files

        Returns:
            List of FileInfo objects
        """
        if path.startswith("gs://"):
            return self._list_gcs_files(path, pattern)
        elif path.startswith("s3://"):
            return self._list_s3_files(path, pattern)
        else:
            return self._list_local_files(path, pattern)

    def _list_gcs_files(
        self,
        path: str,
        pattern: Optional[str] = None,
    ) -> List[FileInfo]:
        """List files in GCS."""
        client = self._get_gcs_client()
        bucket_name, prefix = self._parse_gcs_path(path)
        bucket = client.bucket(bucket_name)

        files = []
        for blob in bucket.list_blobs(prefix=prefix):
            if pattern:
                if not re.match(pattern, blob.name.split("/")[-1]):
                    continue
            files.append(FileInfo(
                path=f"gs://{bucket_name}/{blob.name}",
                name=blob.name.split("/")[-1],
                size_bytes=blob.size,
                modified_time=blob.updated,
                checksum=blob.md5_hash,
            ))

        return files

    def _list_local_files(
        self,
        path: str,
        pattern: Optional[str] = None,
    ) -> List[FileInfo]:
        """List files in local filesystem."""
        files = []

        if os.path.isfile(path):
            stat = os.stat(path)
            files.append(FileInfo(
                path=path,
                name=os.path.basename(path),
                size_bytes=stat.st_size,
                modified_time=datetime.fromtimestamp(stat.st_mtime),
            ))
        elif os.path.isdir(path):
            for name in os.listdir(path):
                if pattern and not re.match(pattern, name):
                    continue
                full_path = os.path.join(path, name)
                if os.path.isfile(full_path):
                    stat = os.stat(full_path)
                    files.append(FileInfo(
                        path=full_path,
                        name=name,
                        size_bytes=stat.st_size,
                        modified_time=datetime.fromtimestamp(stat.st_mtime),
                    ))

        return files

    def _list_s3_files(
        self,
        path: str,
        pattern: Optional[str] = None,
    ) -> List[FileInfo]:
        """List files in S3."""
        # Placeholder for S3 implementation
        return []

    # ═══════════════════════════════════════════════════════════════════════
    # FILE OPERATIONS
    # ═══════════════════════════════════════════════════════════════════════

    def copy_to_transient(
        self,
        source_path: str,
        transient_path: str,
        execution_id: str,
    ) -> Dict[str, Any]:
        """
        Copy source file(s) to transient location.

        Args:
            source_path: Source path
            transient_path: Transient staging path
            execution_id: Execution ID for tracking

        Returns:
            Dict with copy results
        """
        files = self.list_files(source_path)
        copied_files = []

        for file_info in files:
            dest_path = os.path.join(
                transient_path,
                execution_id,
                file_info.name
            )
            self._copy_file(file_info.path, dest_path)
            copied_files.append(dest_path)

        return {
            "source_files": len(files),
            "copied_files": copied_files,
            "total_bytes": sum(f.size_bytes for f in files),
        }

    def move_to_raw(
        self,
        transient_path: str,
        raw_path: str,
        execution_id: str,
    ) -> Dict[str, Any]:
        """
        Move files from transient to raw zone.

        Args:
            transient_path: Transient staging path
            raw_path: Raw zone destination path
            execution_id: Execution ID for tracking

        Returns:
            Dict with move results
        """
        source = os.path.join(transient_path, execution_id)
        files = self.list_files(source)
        moved_files = []

        for file_info in files:
            dest_path = os.path.join(raw_path, execution_id, file_info.name)
            self._move_file(file_info.path, dest_path)
            moved_files.append(dest_path)

        return {
            "moved_files": moved_files,
            "total_bytes": sum(f.size_bytes for f in files),
        }

    def archive_processed_files(
        self,
        source_path: str,
        archive_path: str,
        execution_date: datetime,
    ) -> Dict[str, Any]:
        """Archive processed files."""
        date_folder = execution_date.strftime("%Y/%m/%d")
        archive_dest = os.path.join(archive_path, date_folder)

        files = self.list_files(source_path)
        archived = []

        for file_info in files:
            dest = os.path.join(archive_dest, file_info.name)
            self._move_file(file_info.path, dest)
            archived.append(dest)

        return {"archived_files": archived}

    def _copy_file(self, source: str, destination: str) -> None:
        """Copy a file."""
        if source.startswith("gs://"):
            self._copy_gcs_file(source, destination)
        elif source.startswith("s3://"):
            self._copy_s3_file(source, destination)
        else:
            self._copy_local_file(source, destination)

    def _move_file(self, source: str, destination: str) -> None:
        """Move a file."""
        self._copy_file(source, destination)
        self._delete_file(source)

    def _delete_file(self, path: str) -> None:
        """Delete a file."""
        if path.startswith("gs://"):
            client = self._get_gcs_client()
            bucket_name, blob_name = self._parse_gcs_path(path)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        elif not path.startswith("s3://"):
            os.remove(path)

    def _copy_gcs_file(self, source: str, destination: str) -> None:
        """Copy file in/to GCS."""
        client = self._get_gcs_client()
        src_bucket, src_blob = self._parse_gcs_path(source)
        dst_bucket, dst_blob = self._parse_gcs_path(destination)

        source_bucket = client.bucket(src_bucket)
        source_blob = source_bucket.blob(src_blob)
        dest_bucket = client.bucket(dst_bucket)

        source_bucket.copy_blob(source_blob, dest_bucket, dst_blob)

    def _copy_local_file(self, source: str, destination: str) -> None:
        """Copy local file."""
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.copy2(source, destination)

    def _copy_s3_file(self, source: str, destination: str) -> None:
        """Copy file in S3."""
        # Placeholder for S3 implementation
        pass
