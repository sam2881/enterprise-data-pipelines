"""
APEX Data Agent - Legacy Source Utilities

COBOL copybook parsing, EBCDIC conversion, fixed-width file processing.
"""

from typing import Any, Dict, List, Optional


def parse_copybook(copybook_path: str, **context) -> List[Dict[str, Any]]:
    """
    Parse COBOL copybook to extract field definitions.
    Returns list of {"name": str, "type": str, "length": int, "offset": int}.
    """
    from google.cloud import storage
    import re

    # Download copybook from GCS
    if copybook_path.startswith("gs://"):
        bucket_name = copybook_path.replace("gs://", "").split("/")[0]
        blob_path = "/".join(copybook_path.replace("gs://", "").split("/")[1:])
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        content = blob.download_as_text()
    else:
        with open(copybook_path, "r") as f:
            content = f.read()

    fields = []
    offset = 0
    pic_pattern = re.compile(
        r"^\s+\d+\s+(\S+)\s+PIC\s+(.+?)\.\s*$", re.IGNORECASE | re.MULTILINE
    )

    for match in pic_pattern.finditer(content):
        name = match.group(1).replace("-", "_").lower()
        pic = match.group(2).strip()

        length, field_type = _parse_pic_clause(pic)
        fields.append({
            "name": name,
            "type": field_type,
            "pic": pic,
            "length": length,
            "offset": offset,
        })
        offset += length

    if context.get("ti"):
        context["ti"].xcom_push(key="copybook_fields", value=fields)
        context["ti"].xcom_push(key="record_length", value=offset)

    print(f"[legacy_source] Parsed copybook: {len(fields)} fields, record length={offset}")
    return fields


def _parse_pic_clause(pic: str):
    """Parse COBOL PIC clause to determine type and length."""
    import re

    pic_upper = pic.upper().strip()

    # Numeric: 9(n), S9(n)V9(n), S9(n)V99 COMP-3
    if "COMP-3" in pic_upper or "COMP3" in pic_upper:
        digits = sum(int(m) for m in re.findall(r"\((\d+)\)", pic_upper))
        if not digits:
            digits = pic_upper.count("9")
        length = (digits + 2) // 2  # Packed decimal
        return length, "DECIMAL"

    if "COMP" in pic_upper:
        digits = sum(int(m) for m in re.findall(r"\((\d+)\)", pic_upper))
        if digits <= 4:
            return 2, "INTEGER"
        elif digits <= 9:
            return 4, "INTEGER"
        else:
            return 8, "BIGINT"

    # Alphanumeric: X(n)
    x_match = re.search(r"X\((\d+)\)", pic_upper)
    if x_match:
        return int(x_match.group(1)), "STRING"
    if pic_upper.startswith("X"):
        return pic_upper.count("X"), "STRING"

    # Numeric display: 9(n)
    nine_match = re.findall(r"9\((\d+)\)", pic_upper)
    if nine_match:
        total_digits = sum(int(m) for m in nine_match)
        if "V" in pic_upper:
            return total_digits + 1, "DECIMAL"
        return total_digits, "INTEGER"

    nines = pic_upper.count("9")
    if nines > 0:
        if "V" in pic_upper:
            return nines + 1, "DECIMAL"
        return nines, "INTEGER"

    return len(pic_upper), "STRING"


def convert_ebcdic_to_utf8(
    input_path: str,
    output_path: str,
    copybook_path: Optional[str] = None,
    encoding: str = "cp037",
    **context,
) -> str:
    """Convert EBCDIC file to UTF-8 using Cobrix or manual conversion."""
    print(f"[legacy_source] Converting EBCDIC: {input_path} → {output_path} (encoding={encoding})")

    # The actual conversion happens in the Spark job (source_to_landing.py)
    # using Cobrix format reader. This utility stages the config.
    conversion_config = {
        "input_path": input_path,
        "output_path": output_path,
        "copybook_path": copybook_path,
        "encoding": encoding,
    }

    if context.get("ti"):
        context["ti"].xcom_push(key="ebcdic_config", value=conversion_config)

    return output_path


def read_fixed_width(path: str, field_widths: List[int], field_names: List[str], **context) -> str:
    """
    Read fixed-width file and convert to delimited format.
    Returns path to converted file.
    """
    from google.cloud import storage

    if not path.startswith("gs://"):
        print("[legacy_source] Fixed-width conversion only supported for GCS paths in Spark jobs")
        return path

    # Fixed-width parsing is handled in the Spark job via PySpark substring operations
    # This utility prepares the parsing config for the Spark job
    parsing_config = {
        "field_widths": field_widths,
        "field_names": field_names,
        "total_width": sum(field_widths),
    }

    if context.get("ti"):
        context["ti"].xcom_push(key="fixed_width_config", value=parsing_config)

    return path


def generate_migration_report(
    metadata_db_url: str,
    feed_id: str,
    source_type: str = "EBCDIC",
    **context,
) -> Dict[str, Any]:
    """Generate migration report for legacy source processing."""
    report = {
        "feed_id": feed_id,
        "source_type": source_type,
        "status": "MIGRATED",
        "fields_parsed": context.get("ti", _DummyTi()).xcom_pull(key="copybook_fields") or [],
        "record_length": context.get("ti", _DummyTi()).xcom_pull(key="record_length") or 0,
    }

    print(f"[legacy_source] Migration report: {source_type} feed {feed_id}")
    return report


class _DummyTi:
    def xcom_pull(self, **kwargs):
        return None

    def xcom_push(self, **kwargs):
        pass
