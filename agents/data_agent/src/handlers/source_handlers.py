"""
Dynamic Source Handlers - Multi-format data ingestion.

WHY: Enterprise data comes in many formats:
- CSV with various delimiters and encodings
- CSV with sidecar metadata files
- EBCDIC/Copybook from mainframes (COBOL)
- Fixed-width text files
- JSON/JSONL
- Parquet/Avro/ORC
- Database tables
- REST APIs
- Streaming (Kafka, Pub/Sub)

HOW: Each handler:
1. Detects format automatically or from config
2. Parses schema from source (or copybook)
3. Returns Spark DataFrame with proper types
4. Handles encoding, compression, multiline

References:
- AWS Mainframe Data Utilities: https://github.com/aws-samples/mainframe-data-utilities
- Cobrix (Spark EBCDIC): https://github.com/AbsaOSS/cobrix
"""

import hashlib
import json
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger()


# =============================================================================
# Source Handler Configuration
# =============================================================================


@dataclass
class SourceConfig:
    """Base configuration for all source handlers."""
    source_path: str
    source_format: str
    encoding: str = "utf-8"
    compression: Optional[str] = None  # gzip, snappy, lz4, bzip2
    partition_columns: List[str] = field(default_factory=list)


@dataclass
class CSVConfig(SourceConfig):
    """CSV-specific configuration."""
    delimiter: str = ","
    quote_char: str = '"'
    escape_char: str = "\\"
    header: bool = True
    header_row: int = 0
    skip_rows: int = 0
    null_value: str = ""
    date_format: str = "yyyy-MM-dd"
    timestamp_format: str = "yyyy-MM-dd HH:mm:ss"
    multiline: bool = False
    infer_schema: bool = True
    schema_path: Optional[str] = None  # For CSV with metadata


@dataclass
class EBCDICConfig(SourceConfig):
    """EBCDIC/Copybook configuration for mainframe data."""
    copybook_content: str = ""  # COBOL copybook definition
    copybook_path: Optional[str] = None  # Or path to copybook file
    code_page: str = "cp037"  # IBM US = cp037, International = cp500
    record_format: str = "fixed"  # fixed, variable
    record_length: Optional[int] = None  # For fixed-length records
    rdw_format: str = "rdw"  # Record Descriptor Word format
    binary_fields: List[str] = field(default_factory=list)  # COMP, COMP-3 fields
    string_trimming: bool = True
    generate_record_id: bool = True


@dataclass
class FixedWidthConfig(SourceConfig):
    """Fixed-width text file configuration."""
    column_specs: List[Dict[str, Any]] = field(default_factory=list)
    # [{name: "field1", start: 0, length: 10, type: "string"}]
    record_length: int = 0
    skip_header_lines: int = 0
    skip_footer_lines: int = 0


@dataclass
class JSONConfig(SourceConfig):
    """JSON/JSONL configuration."""
    multiline: bool = False  # True for pretty-printed JSON
    root_path: Optional[str] = None  # JSONPath to data array
    flatten_nested: bool = False
    max_nesting_depth: int = 3


@dataclass
class DatabaseConfig(SourceConfig):
    """Database source configuration."""
    connection_secret: str = ""
    database_type: str = "postgresql"  # postgresql, mysql, oracle, sqlserver
    database_name: str = ""
    schema_name: str = "public"
    table_name: str = ""
    query: Optional[str] = None  # Custom SQL query
    fetch_size: int = 10000
    partition_column: Optional[str] = None
    lower_bound: Optional[int] = None
    upper_bound: Optional[int] = None
    num_partitions: int = 4


@dataclass
class APIConfig(SourceConfig):
    """REST API configuration."""
    endpoint_url: str = ""
    http_method: str = "GET"
    auth_secret: str = ""
    auth_type: str = "bearer"  # bearer, basic, api_key, oauth2
    headers: Dict[str, str] = field(default_factory=dict)
    query_params: Dict[str, str] = field(default_factory=dict)
    body: Optional[Dict[str, Any]] = None
    pagination_type: Optional[str] = None  # offset, cursor, link
    pagination_params: Dict[str, Any] = field(default_factory=dict)
    rate_limit_rps: int = 10
    timeout_seconds: int = 30
    max_pages: int = 100


@dataclass
class StreamingConfig(SourceConfig):
    """Kafka/Pub/Sub streaming configuration."""
    topic_name: str = ""
    bootstrap_servers: str = ""  # For Kafka
    project_id: str = ""  # For Pub/Sub
    subscription_name: Optional[str] = None
    consumer_group: str = "data-agent"
    offset_reset: str = "earliest"  # earliest, latest
    message_format: str = "json"  # json, avro, protobuf
    schema_registry_url: Optional[str] = None
    max_records_per_batch: int = 10000


# =============================================================================
# Parsed Schema
# =============================================================================


@dataclass
class ParsedColumn:
    """Parsed column definition."""
    name: str
    data_type: str  # Spark SQL type
    nullable: bool = True
    description: Optional[str] = None
    start_position: Optional[int] = None  # For fixed-width
    length: Optional[int] = None  # For fixed-width/EBCDIC
    scale: Optional[int] = None  # For decimal
    precision: Optional[int] = None  # For decimal
    original_type: Optional[str] = None  # Original source type


@dataclass
class ParsedSchema:
    """Complete parsed schema."""
    columns: List[ParsedColumn]
    record_length: Optional[int] = None
    source_format: str = ""
    schema_hash: str = ""

    def to_spark_schema(self) -> str:
        """Generate Spark StructType DDL."""
        fields = []
        for col in self.columns:
            nullable = "NULL" if col.nullable else "NOT NULL"
            fields.append(f"`{col.name}` {col.data_type} {nullable}")
        return ", ".join(fields)

    def to_json(self) -> str:
        """Serialize to JSON for storage."""
        return json.dumps({
            "columns": [
                {
                    "name": c.name,
                    "data_type": c.data_type,
                    "nullable": c.nullable,
                    "description": c.description,
                    "start_position": c.start_position,
                    "length": c.length,
                }
                for c in self.columns
            ],
            "record_length": self.record_length,
            "source_format": self.source_format,
        })

    def compute_hash(self) -> str:
        """Compute SHA256 hash for schema drift detection."""
        content = json.dumps(
            [(c.name, c.data_type, c.nullable) for c in self.columns],
            sort_keys=True
        )
        return hashlib.sha256(content.encode()).hexdigest()


# =============================================================================
# Abstract Base Handler
# =============================================================================


class BaseSourceHandler(ABC):
    """
    Abstract base class for all source handlers.

    WHY: Uniform interface enables:
    - Pluggable source types
    - Consistent error handling
    - Schema inference
    - Audit logging
    """

    def __init__(self, config: SourceConfig):
        self.config = config
        self.logger = logger.bind(handler=self.__class__.__name__)

    @abstractmethod
    def parse_schema(self) -> ParsedSchema:
        """Parse and return schema from source."""
        pass

    @abstractmethod
    def generate_read_code(self) -> str:
        """Generate PySpark code to read the source."""
        pass

    @abstractmethod
    def validate_source(self) -> Tuple[bool, List[str]]:
        """Validate source accessibility and format."""
        pass

    def get_spark_options(self) -> Dict[str, str]:
        """Get Spark reader options as dict."""
        return {}


# =============================================================================
# CSV Handler
# =============================================================================


class CSVSourceHandler(BaseSourceHandler):
    """
    Handler for CSV files with various configurations.

    Supports:
    - Standard CSV with header
    - CSV with sidecar schema file
    - Various delimiters and encodings
    - Multiline values
    """

    def __init__(self, config: CSVConfig):
        super().__init__(config)
        self.config: CSVConfig = config

    def parse_schema(self) -> ParsedSchema:
        """Parse schema from CSV header or metadata file."""
        self.logger.info("Parsing CSV schema", path=self.config.source_path)

        columns = []

        # Check for sidecar schema file
        if self.config.schema_path:
            columns = self._parse_schema_file(self.config.schema_path)
        elif self.config.infer_schema:
            # Will be inferred by Spark
            columns = [ParsedColumn(name="_inferred", data_type="STRING")]
        else:
            raise ValueError("No schema source specified for CSV")

        schema = ParsedSchema(
            columns=columns,
            source_format="csv",
        )
        schema.schema_hash = schema.compute_hash()
        return schema

    def _parse_schema_file(self, schema_path: str) -> List[ParsedColumn]:
        """Parse schema from metadata file (JSON or CSV)."""
        # This would read the schema file from GCS/S3
        # For now, return placeholder
        return []

    def generate_read_code(self) -> str:
        """Generate PySpark CSV read code."""
        options = self.get_spark_options()
        options_str = ",\n        ".join([f'"{k}", "{v}"' for k, v in options.items()])

        return f'''
# Read CSV from {self.config.source_path}
df = spark.read \\
    .format("csv") \\
    .options(
        {options_str}
    ) \\
    .load("{self.config.source_path}")

# Add ingestion metadata
df = df.withColumn("_source_file", F.input_file_name()) \\
       .withColumn("_ingested_at", F.current_timestamp()) \\
       .withColumn("_batch_id", F.lit(batch_id))
'''

    def get_spark_options(self) -> Dict[str, str]:
        """Get Spark CSV reader options."""
        return {
            "header": str(self.config.header).lower(),
            "delimiter": self.config.delimiter,
            "quote": self.config.quote_char,
            "escape": self.config.escape_char,
            "nullValue": self.config.null_value,
            "dateFormat": self.config.date_format,
            "timestampFormat": self.config.timestamp_format,
            "multiLine": str(self.config.multiline).lower(),
            "inferSchema": str(self.config.infer_schema).lower(),
            "encoding": self.config.encoding,
        }

    def validate_source(self) -> Tuple[bool, List[str]]:
        """Validate CSV source."""
        errors = []
        # Would check file existence, format, etc.
        return len(errors) == 0, errors


# =============================================================================
# EBCDIC/Copybook Handler (Mainframe)
# =============================================================================


class EBCDICSourceHandler(BaseSourceHandler):
    """
    Handler for EBCDIC files with COBOL copybook schema.

    Supports:
    - Fixed-length records
    - Variable-length records (RDW)
    - COMP (binary) fields
    - COMP-3 (packed decimal) fields
    - REDEFINES
    - OCCURS (arrays)

    Uses Cobrix for Spark or AWS mainframe-data-utilities patterns.

    Reference: https://github.com/AbsaOSS/cobrix
    """

    # COBOL PIC clause to Spark type mapping
    COBOL_TYPE_MAP = {
        "X": "STRING",           # Alphanumeric
        "A": "STRING",           # Alphabetic
        "9": "DECIMAL",          # Numeric
        "S9": "DECIMAL",         # Signed numeric
        "V": "DECIMAL",          # Implied decimal
        "COMP": "LONG",          # Binary
        "COMP-1": "FLOAT",       # Single precision
        "COMP-2": "DOUBLE",      # Double precision
        "COMP-3": "DECIMAL",     # Packed decimal
        "COMP-5": "LONG",        # Native binary
    }

    def __init__(self, config: EBCDICConfig):
        super().__init__(config)
        self.config: EBCDICConfig = config

    def parse_schema(self) -> ParsedSchema:
        """Parse schema from COBOL copybook."""
        self.logger.info("Parsing COBOL copybook")

        copybook = self.config.copybook_content
        if self.config.copybook_path and not copybook:
            # Would read from GCS/S3
            pass

        columns = self._parse_copybook(copybook)

        schema = ParsedSchema(
            columns=columns,
            record_length=self.config.record_length,
            source_format="ebcdic_copybook",
        )
        schema.schema_hash = schema.compute_hash()
        return schema

    def _parse_copybook(self, copybook: str) -> List[ParsedColumn]:
        """
        Parse COBOL copybook into column definitions.

        Example copybook:
            01  CUSTOMER-RECORD.
                05  CUST-ID            PIC 9(10).
                05  CUST-NAME          PIC X(50).
                05  CUST-BALANCE       PIC S9(9)V99 COMP-3.
                05  CUST-STATUS        PIC X(1).
        """
        columns = []
        position = 0

        # Regex patterns for COBOL PIC clauses
        pic_pattern = re.compile(
            r'^\s*(\d{2})\s+(\S+)\s+PIC\s+([SXA9V\(\)]+)(?:\s+(COMP(?:-[1-5])?))?\s*\.',
            re.IGNORECASE | re.MULTILINE
        )

        for match in pic_pattern.finditer(copybook):
            level = int(match.group(1))
            name = match.group(2).replace("-", "_").lower()
            pic_clause = match.group(3).upper()
            comp_type = match.group(4).upper() if match.group(4) else None

            # Skip group levels (01, 05 without PIC) - only process data items
            if level in [1, 66, 77, 88]:
                continue

            # Parse PIC clause
            data_type, length, scale = self._parse_pic_clause(pic_clause, comp_type)

            columns.append(ParsedColumn(
                name=name,
                data_type=data_type,
                nullable=True,
                start_position=position,
                length=length,
                scale=scale,
                original_type=f"PIC {pic_clause}" + (f" {comp_type}" if comp_type else ""),
            ))

            position += length

        return columns

    def _parse_pic_clause(
        self,
        pic_clause: str,
        comp_type: Optional[str]
    ) -> Tuple[str, int, Optional[int]]:
        """
        Parse COBOL PIC clause to determine type and length.

        Examples:
            PIC X(10)       -> STRING, 10 bytes
            PIC 9(5)        -> DECIMAL(5,0), 5 bytes
            PIC S9(7)V99    -> DECIMAL(9,2), 7 bytes
            PIC S9(7)V99 COMP-3 -> DECIMAL(9,2), 5 bytes (packed)
        """
        # Count characters
        x_count = len(re.findall(r'X', pic_clause))
        nine_count = len(re.findall(r'9', pic_clause))

        # Handle repeat notation: X(10) -> 10 X's
        repeat_match = re.search(r'([X9])\((\d+)\)', pic_clause)
        if repeat_match:
            char = repeat_match.group(1)
            count = int(repeat_match.group(2))
            if char == 'X':
                x_count = count
            else:
                nine_count = count

        # Determine type and length
        if 'X' in pic_clause or 'A' in pic_clause:
            # Alphanumeric
            return "STRING", x_count, None

        elif '9' in pic_clause:
            # Numeric
            has_sign = 'S' in pic_clause
            decimal_places = pic_clause.count('V') + len(re.findall(r'9', pic_clause.split('V')[1])) if 'V' in pic_clause else 0

            total_digits = nine_count
            if 'V' in pic_clause:
                # Add decimal digits
                after_v = pic_clause.split('V')[1] if 'V' in pic_clause else ""
                decimal_match = re.search(r'9\((\d+)\)', after_v)
                if decimal_match:
                    decimal_places = int(decimal_match.group(1))
                else:
                    decimal_places = after_v.count('9')

            # Calculate byte length based on COMP type
            if comp_type == "COMP-3":
                # Packed decimal: (digits + 1) / 2 rounded up
                byte_length = (total_digits + decimal_places + 1 + 1) // 2
            elif comp_type in ["COMP", "COMP-5"]:
                # Binary: depends on digit count
                if total_digits <= 4:
                    byte_length = 2
                elif total_digits <= 9:
                    byte_length = 4
                else:
                    byte_length = 8
            else:
                # Display (zoned decimal)
                byte_length = total_digits + decimal_places

            if decimal_places > 0:
                return f"DECIMAL({total_digits + decimal_places}, {decimal_places})", byte_length, decimal_places
            else:
                return "LONG" if comp_type else "INTEGER", byte_length, None

        return "STRING", 1, None

    def generate_read_code(self) -> str:
        """Generate PySpark code for reading EBCDIC files using Cobrix."""
        if self.config.record_format == "variable":
            return self._generate_variable_length_code()
        else:
            return self._generate_fixed_length_code()

    def _generate_fixed_length_code(self) -> str:
        """Generate code for fixed-length EBCDIC records."""
        copybook_escaped = self.config.copybook_content.replace('"', '\\"').replace('\n', '\\n')

        return f'''
# Read EBCDIC file with COBOL copybook using Cobrix
# Reference: https://github.com/AbsaOSS/cobrix

copybook_content = """{self.config.copybook_content}"""

df = spark.read \\
    .format("cobol") \\
    .option("copybook_contents", copybook_content) \\
    .option("encoding", "{self.config.code_page}") \\
    .option("record_format", "F") \\
    .option("string_trimming_policy", "{"both" if self.config.string_trimming else "none"}") \\
    .option("generate_record_id", "{str(self.config.generate_record_id).lower()}") \\
    .load("{self.config.source_path}")

# Flatten any nested structures from COBOL groups
df = df.select("*")

# Add ingestion metadata
df = df.withColumn("_source_file", F.input_file_name()) \\
       .withColumn("_ingested_at", F.current_timestamp()) \\
       .withColumn("_batch_id", F.lit(batch_id)) \\
       .withColumn("_record_id", F.monotonically_increasing_id())
'''

    def _generate_variable_length_code(self) -> str:
        """Generate code for variable-length EBCDIC records (RDW)."""
        return f'''
# Read variable-length EBCDIC file with RDW headers
copybook_content = """{self.config.copybook_content}"""

df = spark.read \\
    .format("cobol") \\
    .option("copybook_contents", copybook_content) \\
    .option("encoding", "{self.config.code_page}") \\
    .option("record_format", "V") \\
    .option("is_rdw_big_endian", "false") \\
    .option("rdw_adjustment", "0") \\
    .option("string_trimming_policy", "both") \\
    .load("{self.config.source_path}")

# Add ingestion metadata
df = df.withColumn("_source_file", F.input_file_name()) \\
       .withColumn("_ingested_at", F.current_timestamp())
'''

    def validate_source(self) -> Tuple[bool, List[str]]:
        """Validate EBCDIC source and copybook."""
        errors = []

        if not self.config.copybook_content and not self.config.copybook_path:
            errors.append("COBOL copybook required for EBCDIC files")

        if self.config.record_format == "fixed" and not self.config.record_length:
            errors.append("Record length required for fixed-format EBCDIC files")

        return len(errors) == 0, errors


# =============================================================================
# Fixed-Width Handler
# =============================================================================


class FixedWidthSourceHandler(BaseSourceHandler):
    """Handler for fixed-width text files (non-EBCDIC)."""

    def __init__(self, config: FixedWidthConfig):
        super().__init__(config)
        self.config: FixedWidthConfig = config

    def parse_schema(self) -> ParsedSchema:
        """Parse schema from column specifications."""
        columns = [
            ParsedColumn(
                name=spec["name"],
                data_type=spec.get("type", "STRING"),
                start_position=spec["start"],
                length=spec["length"],
                nullable=spec.get("nullable", True),
            )
            for spec in self.config.column_specs
        ]

        schema = ParsedSchema(
            columns=columns,
            record_length=self.config.record_length,
            source_format="fixed_width",
        )
        schema.schema_hash = schema.compute_hash()
        return schema

    def generate_read_code(self) -> str:
        """Generate PySpark code for fixed-width files."""
        # Build substring expressions for each column
        select_exprs = []
        for col in self.config.column_specs:
            start = col["start"] + 1  # Spark is 1-indexed
            length = col["length"]
            name = col["name"]
            dtype = col.get("type", "STRING")

            expr = f"F.trim(F.substring(F.col('_raw_line'), {start}, {length}))"
            if dtype != "STRING":
                expr = f"{expr}.cast('{dtype}')"
            select_exprs.append(f"{expr}.alias('{name}')")

        select_str = ",\n        ".join(select_exprs)

        return f'''
# Read fixed-width file
df_raw = spark.read \\
    .text("{self.config.source_path}") \\
    .withColumnRenamed("value", "_raw_line")

# Skip header/footer lines if specified
if {self.config.skip_header_lines} > 0:
    df_raw = df_raw.withColumn("_row_num", F.monotonically_increasing_id())
    df_raw = df_raw.filter(F.col("_row_num") >= {self.config.skip_header_lines})

# Parse columns by position
df = df_raw.select(
        {select_str}
    )

# Add ingestion metadata
df = df.withColumn("_source_file", F.input_file_name()) \\
       .withColumn("_ingested_at", F.current_timestamp())
'''

    def validate_source(self) -> Tuple[bool, List[str]]:
        """Validate fixed-width source."""
        errors = []
        if not self.config.column_specs:
            errors.append("Column specifications required for fixed-width files")
        return len(errors) == 0, errors


# =============================================================================
# JSON Handler
# =============================================================================


class JSONSourceHandler(BaseSourceHandler):
    """Handler for JSON and JSON Lines files."""

    def __init__(self, config: JSONConfig):
        super().__init__(config)
        self.config: JSONConfig = config

    def parse_schema(self) -> ParsedSchema:
        """Infer schema from JSON sample."""
        # Schema will be inferred by Spark
        return ParsedSchema(
            columns=[ParsedColumn(name="_inferred", data_type="STRUCT")],
            source_format="json",
        )

    def generate_read_code(self) -> str:
        """Generate PySpark JSON read code."""
        options = {
            "multiLine": str(self.config.multiline).lower(),
        }

        if self.config.compression:
            options["compression"] = self.config.compression

        options_str = ",\n        ".join([f'"{k}", "{v}"' for k, v in options.items()])

        root_path_code = ""
        if self.config.root_path:
            root_path_code = f'''
# Extract data from nested path: {self.config.root_path}
df = df.select(F.explode(F.col("{self.config.root_path}")).alias("_data"))
df = df.select("_data.*")
'''

        flatten_code = ""
        if self.config.flatten_nested:
            flatten_code = f'''
# Flatten nested structures (max depth: {self.config.max_nesting_depth})
def flatten_df(df, max_depth={self.config.max_nesting_depth}):
    from pyspark.sql.types import StructType, ArrayType
    flat_cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            for sub_field in field.dataType.fields:
                flat_cols.append(F.col(f"{{field.name}}.{{sub_field.name}}").alias(f"{{field.name}}_{{sub_field.name}}"))
        else:
            flat_cols.append(F.col(field.name))
    return df.select(flat_cols)

df = flatten_df(df)
'''

        return f'''
# Read JSON from {self.config.source_path}
df = spark.read \\
    .format("json") \\
    .options(
        {options_str}
    ) \\
    .load("{self.config.source_path}")
{root_path_code}
{flatten_code}
# Add ingestion metadata
df = df.withColumn("_source_file", F.input_file_name()) \\
       .withColumn("_ingested_at", F.current_timestamp())
'''

    def validate_source(self) -> Tuple[bool, List[str]]:
        """Validate JSON source."""
        return True, []


# =============================================================================
# Database Handler
# =============================================================================


class DatabaseSourceHandler(BaseSourceHandler):
    """Handler for database table sources via JDBC."""

    # JDBC URL templates
    JDBC_TEMPLATES = {
        "postgresql": "jdbc:postgresql://{host}:{port}/{database}",
        "mysql": "jdbc:mysql://{host}:{port}/{database}",
        "oracle": "jdbc:oracle:thin:@{host}:{port}/{database}",
        "sqlserver": "jdbc:sqlserver://{host}:{port};databaseName={database}",
    }

    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self.config: DatabaseConfig = config

    def parse_schema(self) -> ParsedSchema:
        """Schema will be fetched from database."""
        return ParsedSchema(
            columns=[ParsedColumn(name="_from_db", data_type="INFERRED")],
            source_format="database",
        )

    def generate_read_code(self) -> str:
        """Generate PySpark JDBC read code."""
        jdbc_url = self.JDBC_TEMPLATES.get(
            self.config.database_type,
            "jdbc:postgresql://{host}:{port}/{database}"
        )

        # Partition clause for parallel reads
        partition_clause = ""
        if self.config.partition_column:
            partition_clause = f'''
    .option("partitionColumn", "{self.config.partition_column}") \\
    .option("lowerBound", "{self.config.lower_bound}") \\
    .option("upperBound", "{self.config.upper_bound}") \\
    .option("numPartitions", "{self.config.num_partitions}") \\'''

        # Table or query
        dbtable = f'"{self.config.schema_name}"."{self.config.table_name}"'
        if self.config.query:
            dbtable = f"({self.config.query}) AS query_result"

        return f'''
# Read from {self.config.database_type} database
# Credentials loaded from Secret Manager: {self.config.connection_secret}

from google.cloud import secretmanager
import json

def get_db_credentials(secret_name: str) -> dict:
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=secret_name)
    return json.loads(response.payload.data.decode("utf-8"))

credentials = get_db_credentials("{self.config.connection_secret}")

jdbc_url = "{jdbc_url}".format(**credentials)

df = spark.read \\
    .format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("dbtable", {dbtable}) \\
    .option("user", credentials["username"]) \\
    .option("password", credentials["password"]) \\
    .option("fetchsize", "{self.config.fetch_size}") \\
    .option("driver", "{self._get_driver()}") \\{partition_clause}
    .load()

# Add ingestion metadata
df = df.withColumn("_source_table", F.lit("{self.config.table_name}")) \\
       .withColumn("_ingested_at", F.current_timestamp())
'''

    def _get_driver(self) -> str:
        """Get JDBC driver class name."""
        drivers = {
            "postgresql": "org.postgresql.Driver",
            "mysql": "com.mysql.cj.jdbc.Driver",
            "oracle": "oracle.jdbc.OracleDriver",
            "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }
        return drivers.get(self.config.database_type, "org.postgresql.Driver")

    def validate_source(self) -> Tuple[bool, List[str]]:
        """Validate database source."""
        errors = []
        if not self.config.connection_secret:
            errors.append("Connection secret required for database source")
        if not self.config.table_name and not self.config.query:
            errors.append("Table name or query required")
        return len(errors) == 0, errors


# =============================================================================
# API Handler
# =============================================================================


class APISourceHandler(BaseSourceHandler):
    """Handler for REST API data sources."""

    def __init__(self, config: APIConfig):
        super().__init__(config)
        self.config: APIConfig = config

    def parse_schema(self) -> ParsedSchema:
        """Schema inferred from API response."""
        return ParsedSchema(
            columns=[ParsedColumn(name="_from_api", data_type="JSON")],
            source_format="api",
        )

    def generate_read_code(self) -> str:
        """Generate PySpark code for API ingestion."""
        pagination_code = self._generate_pagination_code()

        return f'''
# Fetch data from REST API: {self.config.endpoint_url}
import requests
import json
import time
from google.cloud import secretmanager
from pyspark.sql.types import *

def get_api_credentials(secret_name: str) -> dict:
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=secret_name)
    return json.loads(response.payload.data.decode("utf-8"))

credentials = get_api_credentials("{self.config.auth_secret}")

def fetch_api_data():
    """Fetch all data from API with pagination and rate limiting."""
    all_data = []
    headers = {self.config.headers}

    # Add authentication
    auth_type = "{self.config.auth_type}"
    if auth_type == "bearer":
        headers["Authorization"] = f"Bearer {{credentials.get('token', '')}}"
    elif auth_type == "api_key":
        headers["X-API-Key"] = credentials.get("api_key", "")

    url = "{self.config.endpoint_url}"
    page = 1
    max_pages = {self.config.max_pages}
{pagination_code}

    return all_data

# Fetch data
api_data = fetch_api_data()

# Convert to DataFrame
if api_data:
    df = spark.createDataFrame(api_data)
else:
    df = spark.createDataFrame([], schema=StructType([]))

# Add ingestion metadata
df = df.withColumn("_source_api", F.lit("{self.config.endpoint_url}")) \\
       .withColumn("_ingested_at", F.current_timestamp()) \\
       .withColumn("_batch_id", F.lit(batch_id))
'''

    def _generate_pagination_code(self) -> str:
        """Generate pagination handling code."""
        if not self.config.pagination_type:
            return '''
    response = requests.request(
        method="{method}",
        url=url,
        headers=headers,
        timeout={timeout}
    )
    response.raise_for_status()
    data = response.json()
    if isinstance(data, list):
        all_data.extend(data)
    elif isinstance(data, dict) and "data" in data:
        all_data.extend(data["data"])
    else:
        all_data.append(data)
'''.format(method=self.config.http_method, timeout=self.config.timeout_seconds)

        if self.config.pagination_type == "offset":
            return f'''
    page_size = {self.config.pagination_params.get("page_size", 100)}
    offset = 0

    while page <= max_pages:
        params = {self.config.query_params}
        params["limit"] = page_size
        params["offset"] = offset

        response = requests.request(
            method="{self.config.http_method}",
            url=url,
            headers=headers,
            params=params,
            timeout={self.config.timeout_seconds}
        )
        response.raise_for_status()
        data = response.json()

        records = data.get("data", data) if isinstance(data, dict) else data
        if not records:
            break

        all_data.extend(records)
        offset += page_size
        page += 1

        # Rate limiting
        time.sleep(1.0 / {self.config.rate_limit_rps})
'''

        elif self.config.pagination_type == "cursor":
            return f'''
    cursor = None

    while page <= max_pages:
        params = {self.config.query_params}
        if cursor:
            params["cursor"] = cursor

        response = requests.request(
            method="{self.config.http_method}",
            url=url,
            headers=headers,
            params=params,
            timeout={self.config.timeout_seconds}
        )
        response.raise_for_status()
        data = response.json()

        records = data.get("data", [])
        all_data.extend(records)

        cursor = data.get("next_cursor") or data.get("cursor")
        if not cursor:
            break

        page += 1
        time.sleep(1.0 / {self.config.rate_limit_rps})
'''

        return ""

    def validate_source(self) -> Tuple[bool, List[str]]:
        """Validate API source."""
        errors = []
        if not self.config.endpoint_url:
            errors.append("API endpoint URL required")
        if not self.config.auth_secret:
            errors.append("Authentication secret required")
        return len(errors) == 0, errors


# =============================================================================
# Streaming Handler (Kafka/Pub/Sub)
# =============================================================================


class StreamingSourceHandler(BaseSourceHandler):
    """Handler for Kafka and Pub/Sub streaming sources."""

    def __init__(self, config: StreamingConfig):
        super().__init__(config)
        self.config: StreamingConfig = config

    def parse_schema(self) -> ParsedSchema:
        """Schema from schema registry or inferred."""
        return ParsedSchema(
            columns=[ParsedColumn(name="_message", data_type="BINARY")],
            source_format="streaming",
        )

    def generate_read_code(self) -> str:
        """Generate PySpark Structured Streaming code."""
        if "kafka" in self.config.source_format.lower():
            return self._generate_kafka_code()
        else:
            return self._generate_pubsub_code()

    def _generate_kafka_code(self) -> str:
        """Generate Kafka reader code."""
        schema_registry = ""
        if self.config.schema_registry_url:
            schema_registry = f'''
# Fetch schema from registry
from confluent_kafka.schema_registry import SchemaRegistryClient
schema_registry = SchemaRegistryClient({{"url": "{self.config.schema_registry_url}"}})
schema = schema_registry.get_latest_version("{self.config.topic_name}-value")
'''

        return f'''
# Read from Kafka topic: {self.config.topic_name}
{schema_registry}

df = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "{self.config.bootstrap_servers}") \\
    .option("subscribe", "{self.config.topic_name}") \\
    .option("startingOffsets", "{self.config.offset_reset}") \\
    .option("kafka.group.id", "{self.config.consumer_group}") \\
    .option("maxOffsetsPerTrigger", "{self.config.max_records_per_batch}") \\
    .load()

# Parse message value
df = df.select(
    F.col("key").cast("string").alias("_kafka_key"),
    F.col("value").cast("string").alias("_raw_value"),
    F.col("timestamp").alias("_kafka_timestamp"),
    F.col("partition").alias("_kafka_partition"),
    F.col("offset").alias("_kafka_offset")
)

# Parse JSON value (adjust based on message format)
if "{self.config.message_format}" == "json":
    # Schema inference or use predefined schema
    df = df.withColumn("_parsed", F.from_json(F.col("_raw_value"), schema)) \\
           .select("_kafka_*", "_parsed.*")
'''

    def _generate_pubsub_code(self) -> str:
        """Generate Pub/Sub reader code."""
        return f'''
# Read from Pub/Sub subscription: {self.config.subscription_name}

df = spark.readStream \\
    .format("pubsub") \\
    .option("subscriptionId", "{self.config.subscription_name}") \\
    .option("projectId", "{self.config.project_id}") \\
    .load()

# Parse message
df = df.select(
    F.col("data").cast("string").alias("_raw_value"),
    F.col("publish_time").alias("_pubsub_timestamp"),
    F.col("attributes").alias("_pubsub_attributes")
)

# Parse JSON value
if "{self.config.message_format}" == "json":
    df = df.withColumn("_parsed", F.from_json(F.col("_raw_value"), schema)) \\
           .select("_pubsub_*", "_parsed.*")
'''

    def validate_source(self) -> Tuple[bool, List[str]]:
        """Validate streaming source."""
        errors = []
        if not self.config.topic_name:
            errors.append("Topic name required")
        return len(errors) == 0, errors


# =============================================================================
# Handler Factory
# =============================================================================


class SourceHandlerFactory:
    """
    Factory for creating source handlers based on format.

    WHY: Centralized handler creation enables:
    - Easy extension with new formats
    - Consistent configuration validation
    - Format auto-detection
    """

    HANDLERS: Dict[str, Type[BaseSourceHandler]] = {
        "csv": CSVSourceHandler,
        "csv_with_metadata": CSVSourceHandler,
        "ebcdic_copybook": EBCDICSourceHandler,
        "fixed_width": FixedWidthSourceHandler,
        "json": JSONSourceHandler,
        "jsonl": JSONSourceHandler,
        "database": DatabaseSourceHandler,
        "api": APISourceHandler,
        "streaming_kafka": StreamingSourceHandler,
        "streaming_pubsub": StreamingSourceHandler,
    }

    CONFIG_CLASSES: Dict[str, Type[SourceConfig]] = {
        "csv": CSVConfig,
        "csv_with_metadata": CSVConfig,
        "ebcdic_copybook": EBCDICConfig,
        "fixed_width": FixedWidthConfig,
        "json": JSONConfig,
        "jsonl": JSONConfig,
        "database": DatabaseConfig,
        "api": APIConfig,
        "streaming_kafka": StreamingConfig,
        "streaming_pubsub": StreamingConfig,
    }

    @classmethod
    def create_handler(
        cls,
        source_format: str,
        config_dict: Dict[str, Any]
    ) -> BaseSourceHandler:
        """
        Create a source handler for the given format.

        Args:
            source_format: Format identifier (csv, ebcdic_copybook, etc.)
            config_dict: Configuration dictionary

        Returns:
            Configured source handler
        """
        handler_class = cls.HANDLERS.get(source_format.lower())
        if not handler_class:
            raise ValueError(f"Unsupported source format: {source_format}")

        config_class = cls.CONFIG_CLASSES.get(source_format.lower())
        if not config_class:
            raise ValueError(f"No config class for format: {source_format}")

        # Create config from dict
        config = config_class(**config_dict)

        return handler_class(config)

    @classmethod
    def detect_format(cls, file_path: str) -> str:
        """
        Auto-detect file format from extension and content.

        Args:
            file_path: Path to the file

        Returns:
            Detected format identifier
        """
        path = Path(file_path)
        extension = path.suffix.lower()

        extension_map = {
            ".csv": "csv",
            ".tsv": "csv",
            ".json": "json",
            ".jsonl": "jsonl",
            ".parquet": "parquet",
            ".avro": "avro",
            ".orc": "orc",
            ".txt": "fixed_width",
            ".dat": "ebcdic_copybook",
            ".ebc": "ebcdic_copybook",
        }

        return extension_map.get(extension, "csv")

    @classmethod
    def list_supported_formats(cls) -> List[str]:
        """Return list of supported formats."""
        return list(cls.HANDLERS.keys())
