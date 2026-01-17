"""
Schema Builder - Build Spark schemas dynamically from metadata.

WHY: Schema is defined in metadata, not code. Same utility works for all feeds.
     Bronze layer: ALL columns as STRING (preserve raw data)
     Silver layer: Typed columns from metadata (target_type)

HOW: Reads column definitions from MetadataReader and builds StructType.

DESIGN DECISIONS:
1. Bronze always STRING - no exceptions
2. Silver uses metadata target_type
3. Audit columns added automatically
4. Returns both StructType and DDL string
"""

from typing import List, Optional, Tuple
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    BooleanType,
    DateType,
    TimestampType,
    ArrayType,
    MapType,
)

from .metadata_reader import MetadataReader, ColumnConfig


class SchemaBuilder:
    """
    Build Spark schemas from metadata.

    Usage:
        builder = SchemaBuilder(metadata_reader)
        bronze_schema = builder.build_bronze_schema("customer_transactions")
        silver_schema = builder.build_silver_schema("customer_transactions")
    """

    # Mapping from metadata type strings to Spark types
    TYPE_MAPPING = {
        # String types
        "string": StringType(),
        "varchar": StringType(),
        "char": StringType(),
        "text": StringType(),

        # Integer types
        "int": IntegerType(),
        "integer": IntegerType(),
        "smallint": IntegerType(),
        "tinyint": IntegerType(),
        "bigint": LongType(),
        "long": LongType(),

        # Floating point
        "float": FloatType(),
        "double": DoubleType(),
        "real": FloatType(),

        # Boolean
        "boolean": BooleanType(),
        "bool": BooleanType(),

        # Date/Time
        "date": DateType(),
        "timestamp": TimestampType(),
        "datetime": TimestampType(),

        # Default fallback
        "default": StringType(),
    }

    # Standard audit columns for each layer
    BRONZE_AUDIT_COLUMNS = [
        ("_source_file", StringType(), "Source file path"),
        ("_ingestion_ts", TimestampType(), "Ingestion timestamp"),
        ("_batch_id", StringType(), "Batch identifier"),
        ("_row_hash", StringType(), "MD5 hash of row content"),
    ]

    SILVER_AUDIT_COLUMNS = [
        ("_source_file", StringType(), "Source file path"),
        ("_ingestion_ts", TimestampType(), "Original ingestion timestamp"),
        ("_batch_id", StringType(), "Batch identifier"),
        ("_row_hash", StringType(), "MD5 hash of row content"),
        ("_silver_ts", TimestampType(), "Silver transformation timestamp"),
        ("_is_valid", BooleanType(), "Passed validation"),
    ]

    GOLD_AUDIT_COLUMNS = [
        ("_gold_ts", TimestampType(), "Gold aggregation timestamp"),
        ("_batch_id", StringType(), "Batch identifier"),
    ]

    def __init__(self, metadata_reader: Optional[MetadataReader] = None):
        """Initialize with MetadataReader."""
        self.reader = metadata_reader or MetadataReader()

    def _parse_decimal_type(self, type_str: str) -> DecimalType:
        """Parse decimal(precision, scale) string."""
        # Examples: decimal(18,2), decimal(10,0)
        import re
        match = re.match(r"decimal\((\d+),\s*(\d+)\)", type_str.lower())
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2))
            return DecimalType(precision, scale)
        return DecimalType(18, 2)  # Default

    def _get_spark_type(self, type_str: str):
        """Convert metadata type string to Spark DataType."""
        if type_str is None:
            return StringType()

        type_lower = type_str.lower().strip()

        # Handle decimal with precision
        if type_lower.startswith("decimal"):
            return self._parse_decimal_type(type_lower)

        # Handle varchar with length (treat as string)
        if type_lower.startswith("varchar") or type_lower.startswith("char"):
            return StringType()

        return self.TYPE_MAPPING.get(type_lower, StringType())

    # =========================================================================
    # Bronze Schema (ALL STRING)
    # =========================================================================

    def build_bronze_schema(
        self,
        feed_id: str,
        include_audit: bool = True
    ) -> StructType:
        """
        Build Bronze layer schema - ALL columns as STRING.

        WHY: Bronze preserves raw data exactly. Type casting happens in Silver.
             This prevents data loss from parse errors.

        Args:
            feed_id: Feed identifier
            include_audit: Include audit columns

        Returns:
            StructType with all STRING columns + audit columns
        """
        columns = self.reader.get_columns(feed_id)

        if not columns:
            raise ValueError(f"No columns found for feed: {feed_id}")

        fields = []

        # All business columns as STRING
        for col in columns:
            fields.append(
                StructField(
                    name=col.column_name,
                    dataType=StringType(),  # ALWAYS STRING in Bronze
                    nullable=True,  # Accept everything in Bronze
                    metadata={"description": col.description or ""}
                )
            )

        # Add audit columns
        if include_audit:
            for name, dtype, desc in self.BRONZE_AUDIT_COLUMNS:
                fields.append(
                    StructField(
                        name=name,
                        dataType=dtype,
                        nullable=False,
                        metadata={"description": desc}
                    )
                )

        return StructType(fields)

    def build_bronze_ddl(self, feed_id: str, include_audit: bool = True) -> str:
        """
        Build Bronze DDL string for table creation.

        Returns:
            DDL string like: "col_a STRING, col_b STRING, ..."
        """
        columns = self.reader.get_columns(feed_id)
        parts = []

        for col in columns:
            parts.append(f"{col.column_name} STRING")

        if include_audit:
            for name, dtype, _ in self.BRONZE_AUDIT_COLUMNS:
                spark_type = dtype.simpleString().upper()
                parts.append(f"{name} {spark_type}")

        return ", ".join(parts)

    # =========================================================================
    # Silver Schema (Typed from Metadata)
    # =========================================================================

    def build_silver_schema(
        self,
        feed_id: str,
        include_audit: bool = True
    ) -> StructType:
        """
        Build Silver layer schema - Types from metadata.

        WHY: Silver enforces data types and quality. This is the trusted layer.
             Types come from feed_columns.target_type.

        Args:
            feed_id: Feed identifier
            include_audit: Include audit columns

        Returns:
            StructType with properly typed columns + audit columns
        """
        columns = self.reader.get_columns(feed_id)

        if not columns:
            raise ValueError(f"No columns found for feed: {feed_id}")

        fields = []

        # Business columns with target types
        for col in columns:
            spark_type = self._get_spark_type(col.target_type)
            fields.append(
                StructField(
                    name=col.column_name,
                    dataType=spark_type,
                    nullable=col.is_nullable,
                    metadata={
                        "description": col.description or "",
                        "source_type": col.source_type,
                        "target_type": col.target_type,
                    }
                )
            )

        # Add audit columns
        if include_audit:
            for name, dtype, desc in self.SILVER_AUDIT_COLUMNS:
                fields.append(
                    StructField(
                        name=name,
                        dataType=dtype,
                        nullable=False,
                        metadata={"description": desc}
                    )
                )

        return StructType(fields)

    def build_silver_ddl(self, feed_id: str, include_audit: bool = True) -> str:
        """
        Build Silver DDL string for table creation.

        Returns:
            DDL string like: "col_a INT, col_b DATE, ..."
        """
        columns = self.reader.get_columns(feed_id)
        parts = []

        for col in columns:
            spark_type = self._get_spark_type(col.target_type)
            type_str = spark_type.simpleString().upper()
            nullable = "" if col.is_nullable else " NOT NULL"
            parts.append(f"{col.column_name} {type_str}{nullable}")

        if include_audit:
            for name, dtype, _ in self.SILVER_AUDIT_COLUMNS:
                spark_type = dtype.simpleString().upper()
                parts.append(f"{name} {spark_type}")

        return ", ".join(parts)

    # =========================================================================
    # Type Casting Expressions
    # =========================================================================

    def get_cast_expressions(self, feed_id: str) -> List[Tuple[str, str, str]]:
        """
        Get SQL cast expressions for Bronze → Silver transformation.

        Returns:
            List of (column_name, cast_expression, target_type)

        Example:
            [
                ("amount", "CAST(amount AS DECIMAL(18,2))", "decimal(18,2)"),
                ("transaction_date", "TO_DATE(transaction_date, 'yyyy-MM-dd')", "date"),
            ]
        """
        columns = self.reader.get_columns(feed_id)
        expressions = []

        for col in columns:
            target_type = col.target_type.lower()

            # Use custom transform if provided
            if col.transform_expression:
                expr = col.transform_expression
            # Date parsing
            elif target_type == "date":
                expr = f"TO_DATE({col.column_name}, 'yyyy-MM-dd')"
            # Timestamp parsing
            elif target_type in ("timestamp", "datetime"):
                expr = f"TO_TIMESTAMP({col.column_name}, 'yyyy-MM-dd HH:mm:ss')"
            # Decimal with precision
            elif target_type.startswith("decimal"):
                expr = f"CAST({col.column_name} AS {target_type.upper()})"
            # Simple cast for other types
            else:
                spark_type = self._get_spark_type(target_type)
                expr = f"CAST({col.column_name} AS {spark_type.simpleString().upper()})"

            expressions.append((col.column_name, expr, col.target_type))

        return expressions

    def build_select_expression(self, feed_id: str, layer: str = "silver") -> str:
        """
        Build complete SELECT expression for transformation.

        Args:
            feed_id: Feed identifier
            layer: Target layer (silver, gold)

        Returns:
            SQL SELECT expression with all casts
        """
        if layer == "bronze":
            # Bronze: just select as STRING
            columns = self.reader.get_columns(feed_id)
            return ", ".join([f"CAST({c.column_name} AS STRING) AS {c.column_name}" for c in columns])

        # Silver: apply type casts
        cast_exprs = self.get_cast_expressions(feed_id)
        select_parts = [f"{expr} AS {name}" for name, expr, _ in cast_exprs]
        return ", ".join(select_parts)
