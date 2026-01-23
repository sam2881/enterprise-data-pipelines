"""
COBOL Copybook Parser for EBCDIC File Processing

WHY: Enterprise mainframe data comes in EBCDIC format with COBOL copybook definitions.
     This parser converts copybook to schema for Spark processing via Cobrix.

SUPPORTS:
- PIC X(n) - Alphanumeric
- PIC 9(n) - Numeric display
- PIC S9(n) - Signed numeric
- PIC 9(n)V9(n) - Implied decimal
- COMP-3 (Packed Decimal)
- COMP (Binary)
- REDEFINES
- OCCURS (Arrays)
- FILLER fields
- Level numbers (01-49, 66, 77, 88)

USAGE:
    parser = CopybookParser()
    schema = parser.parse_file('/path/to/copybook.cpy')
    spark_schema = parser.to_spark_schema()
    cobrix_config = parser.to_cobrix_config()
"""

import re
import json
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple
from enum import Enum
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class DataType(Enum):
    """COBOL data types."""
    ALPHANUMERIC = "alphanumeric"     # PIC X
    NUMERIC_DISPLAY = "numeric"        # PIC 9
    PACKED_DECIMAL = "comp3"           # COMP-3
    BINARY = "binary"                  # COMP/COMP-4
    COMP_1 = "float"                   # COMP-1 (4-byte float)
    COMP_2 = "double"                  # COMP-2 (8-byte double)


@dataclass
class CopybookField:
    """Represents a single field in a COBOL copybook."""
    level: int
    name: str
    pic_clause: Optional[str] = None
    data_type: DataType = DataType.ALPHANUMERIC
    length: int = 0
    decimal_places: int = 0
    signed: bool = False
    occurs: int = 1
    redefines: Optional[str] = None
    is_filler: bool = False
    start_position: int = 0
    parent: Optional[str] = None
    children: List['CopybookField'] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'level': self.level,
            'name': self.name,
            'pic_clause': self.pic_clause,
            'data_type': self.data_type.value,
            'length': self.length,
            'decimal_places': self.decimal_places,
            'signed': self.signed,
            'occurs': self.occurs,
            'redefines': self.redefines,
            'is_filler': self.is_filler,
            'start_position': self.start_position,
            'children': [c.to_dict() for c in self.children],
        }


@dataclass
class CopybookSchema:
    """Complete schema parsed from a COBOL copybook."""
    name: str
    record_length: int
    fields: List[CopybookField]
    source_file: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'name': self.name,
            'record_length': self.record_length,
            'source_file': self.source_file,
            'fields': [f.to_dict() for f in self.fields],
        }

    def to_flat_fields(self) -> List[Dict[str, Any]]:
        """Flatten hierarchy for simple field list."""
        flat = []

        def flatten(fields: List[CopybookField], prefix: str = ''):
            for f in fields:
                if f.pic_clause:  # Only leaf nodes have PIC
                    flat.append({
                        'name': f"{prefix}{f.name}" if prefix else f.name,
                        'start': f.start_position,
                        'length': f.length,
                        'type': f.data_type.value,
                        'decimal_places': f.decimal_places,
                        'signed': f.signed,
                        'occurs': f.occurs,
                    })
                if f.children:
                    flatten(f.children, f"{prefix}{f.name}_")

        flatten(self.fields)
        return flat


class CopybookParser:
    """
    Parser for COBOL copybook files.

    Converts copybook definitions to structured schema for:
    - Spark processing via Cobrix
    - Direct Python EBCDIC parsing
    - Schema validation
    """

    # PIC clause patterns
    PIC_ALPHA = re.compile(r'^X\((\d+)\)$|^X+$')
    PIC_NUMERIC = re.compile(r'^S?9\((\d+)\)(V9\((\d+)\))?$|^S?9+(V9+)?$')

    # Line parsing pattern
    LINE_PATTERN = re.compile(
        r'^\s*(\d{2})\s+'  # Level number
        r'([\w-]+)\s*'      # Field name
        r'(REDEFINES\s+[\w-]+\s*)?'  # Optional REDEFINES
        r'(PIC\s+[\w()V.+-]+\s*)?'   # Optional PIC clause
        r'(COMP-?[1-5]?\s*)?'        # Optional COMP
        r'(OCCURS\s+\d+\s*(TIMES)?\s*)?'  # Optional OCCURS
        r'(VALUE\s+.*)?'             # Optional VALUE (ignored)
        r'\.',                       # Period terminator
        re.IGNORECASE
    )

    def __init__(self):
        self.fields: List[CopybookField] = []
        self.record_length: int = 0
        self.current_position: int = 0

    def parse_file(self, file_path: str) -> CopybookSchema:
        """Parse a copybook file and return schema."""
        path = Path(file_path)
        content = path.read_text(encoding='utf-8')
        return self.parse(content, name=path.stem, source_file=str(path))

    def parse(self, content: str, name: str = 'record',
              source_file: Optional[str] = None) -> CopybookSchema:
        """
        Parse copybook content and return schema.

        Args:
            content: Copybook text content
            name: Schema/record name
            source_file: Source file path for reference

        Returns:
            CopybookSchema with parsed fields
        """
        self.fields = []
        self.current_position = 0

        # Preprocess: remove comments, join continuation lines
        lines = self._preprocess(content)

        # Parse each line
        level_stack: List[CopybookField] = []

        for line in lines:
            field = self._parse_line(line)
            if not field:
                continue

            # Handle hierarchy based on level numbers
            while level_stack and level_stack[-1].level >= field.level:
                level_stack.pop()

            if level_stack:
                field.parent = level_stack[-1].name
                level_stack[-1].children.append(field)
            else:
                self.fields.append(field)

            # Calculate position for leaf fields
            if field.pic_clause and not field.redefines:
                field.start_position = self.current_position
                self.current_position += field.length * field.occurs

            if field.level < 49:  # Group levels
                level_stack.append(field)

        self.record_length = self.current_position

        return CopybookSchema(
            name=name,
            record_length=self.record_length,
            fields=self.fields,
            source_file=source_file,
        )

    def _preprocess(self, content: str) -> List[str]:
        """Preprocess copybook content."""
        lines = []
        current_line = ''

        for line in content.split('\n'):
            # Skip comments (column 7 = * or /)
            if len(line) > 6 and line[6] in ('*', '/'):
                continue

            # Remove sequence numbers (columns 1-6) and identification (73-80)
            if len(line) > 72:
                line = line[6:72]
            elif len(line) > 6:
                line = line[6:]
            else:
                continue

            line = line.strip()
            if not line:
                continue

            # Join continuation lines
            current_line += ' ' + line

            # Check if line is complete (ends with period)
            if current_line.strip().endswith('.'):
                lines.append(current_line.strip())
                current_line = ''

        return lines

    def _parse_line(self, line: str) -> Optional[CopybookField]:
        """Parse a single copybook line."""
        # Extract components using regex
        match = self.LINE_PATTERN.match(line)

        if not match:
            # Try simpler parsing
            parts = line.split()
            if len(parts) < 2:
                return None

            try:
                level = int(parts[0])
            except ValueError:
                return None

            name = parts[1].rstrip('.')
            pic_clause = None
            comp_type = None
            occurs = 1
            redefines = None

            # Find PIC clause
            for i, part in enumerate(parts):
                if part.upper() == 'PIC':
                    pic_clause = parts[i + 1].rstrip('.') if i + 1 < len(parts) else None
                elif part.upper().startswith('COMP'):
                    comp_type = part.upper()
                elif part.upper() == 'OCCURS':
                    try:
                        occurs = int(parts[i + 1])
                    except (IndexError, ValueError):
                        occurs = 1
                elif part.upper() == 'REDEFINES':
                    redefines = parts[i + 1] if i + 1 < len(parts) else None

        else:
            level = int(match.group(1))
            name = match.group(2)
            redefines_match = match.group(3)
            pic_match = match.group(4)
            comp_match = match.group(5)
            occurs_match = match.group(6)

            redefines = None
            if redefines_match:
                redefines = redefines_match.replace('REDEFINES', '').strip()

            pic_clause = None
            if pic_match:
                pic_clause = pic_match.replace('PIC', '').strip().rstrip('.')

            comp_type = comp_match.strip().upper() if comp_match else None

            occurs = 1
            if occurs_match:
                occurs_num = re.search(r'\d+', occurs_match)
                occurs = int(occurs_num.group()) if occurs_num else 1

        # Skip 88-level condition names
        if level == 88:
            return None

        # Check for FILLER
        is_filler = name.upper() == 'FILLER'
        if is_filler:
            name = f'FILLER_{self.current_position}'

        # Parse PIC clause for data type and length
        data_type, length, decimal_places, signed = self._parse_pic(
            pic_clause, comp_type
        )

        return CopybookField(
            level=level,
            name=name,
            pic_clause=pic_clause,
            data_type=data_type,
            length=length,
            decimal_places=decimal_places,
            signed=signed,
            occurs=occurs,
            redefines=redefines,
            is_filler=is_filler,
        )

    def _parse_pic(self, pic_clause: Optional[str],
                   comp_type: Optional[str]) -> Tuple[DataType, int, int, bool]:
        """
        Parse PIC clause to determine data type and length.

        Returns:
            Tuple of (data_type, length, decimal_places, signed)
        """
        if not pic_clause:
            return DataType.ALPHANUMERIC, 0, 0, False

        pic = pic_clause.upper().replace(' ', '')
        signed = pic.startswith('S')
        if signed:
            pic = pic[1:]

        decimal_places = 0

        # Check for alphanumeric
        if 'X' in pic:
            # PIC X(10) or PIC XXXX
            match = re.search(r'X\((\d+)\)', pic)
            if match:
                length = int(match.group(1))
            else:
                length = pic.count('X')
            return DataType.ALPHANUMERIC, length, 0, False

        # Numeric with possible decimal
        # Handle V (implied decimal point)
        if 'V' in pic:
            int_part, dec_part = pic.split('V')

            # Integer part
            int_match = re.search(r'9\((\d+)\)', int_part)
            int_len = int(int_match.group(1)) if int_match else int_part.count('9')

            # Decimal part
            dec_match = re.search(r'9\((\d+)\)', dec_part)
            decimal_places = int(dec_match.group(1)) if dec_match else dec_part.count('9')

            total_digits = int_len + decimal_places
        else:
            match = re.search(r'9\((\d+)\)', pic)
            total_digits = int(match.group(1)) if match else pic.count('9')

        # Determine storage length based on COMP type
        if comp_type:
            if 'COMP-3' in comp_type or 'COMP3' in comp_type:
                # Packed decimal: (digits + 1) / 2 bytes
                length = (total_digits + 1 + 1) // 2
                return DataType.PACKED_DECIMAL, length, decimal_places, signed
            elif 'COMP-1' in comp_type or 'COMP1' in comp_type:
                return DataType.COMP_1, 4, 0, True
            elif 'COMP-2' in comp_type or 'COMP2' in comp_type:
                return DataType.COMP_2, 8, 0, True
            elif 'COMP' in comp_type:
                # Binary: depends on total digits
                if total_digits <= 4:
                    length = 2
                elif total_digits <= 9:
                    length = 4
                else:
                    length = 8
                return DataType.BINARY, length, decimal_places, signed

        # Display numeric (one byte per digit + sign)
        length = total_digits + (1 if signed else 0)
        return DataType.NUMERIC_DISPLAY, length, decimal_places, signed

    def to_spark_schema(self, schema: CopybookSchema) -> str:
        """
        Generate PySpark StructType schema from copybook.

        Returns:
            PySpark schema code as string
        """
        lines = ["from pyspark.sql.types import *", "", "schema = StructType(["]

        def add_field(f: CopybookField, indent: int = 4):
            type_map = {
                DataType.ALPHANUMERIC: "StringType()",
                DataType.NUMERIC_DISPLAY: f"DecimalType({f.length}, {f.decimal_places})" if f.decimal_places else "LongType()",
                DataType.PACKED_DECIMAL: f"DecimalType({f.length * 2 - 1}, {f.decimal_places})",
                DataType.BINARY: "LongType()",
                DataType.COMP_1: "FloatType()",
                DataType.COMP_2: "DoubleType()",
            }

            spark_type = type_map.get(f.data_type, "StringType()")

            if f.occurs > 1:
                spark_type = f"ArrayType({spark_type})"

            lines.append(f"{' ' * indent}StructField(\"{f.name}\", {spark_type}, True),")

        for field in schema.fields:
            if field.pic_clause and not field.is_filler:
                add_field(field)
            for child in field.children:
                if child.pic_clause and not child.is_filler:
                    add_field(child, 4)

        lines.append("])")
        return "\n".join(lines)

    def to_cobrix_config(self, schema: CopybookSchema) -> Dict[str, Any]:
        """
        Generate Cobrix configuration for Spark EBCDIC reading.

        Cobrix is the standard library for reading mainframe files in Spark.
        https://github.com/AbsaOSS/cobrix

        Returns:
            Dictionary with Cobrix DataFrame reader options
        """
        return {
            "spark.read.format": "cobol",
            "options": {
                "copybook_contents": self._generate_copybook_string(schema),
                "encoding": "cp037",  # IBM EBCDIC
                "record_format": "F",  # Fixed-length records
                "record_length": schema.record_length,
                "schema_retention_policy": "collapse_root",
                "generate_record_id": True,
                "string_trimming_policy": "both",
            },
            "spark_code": self._generate_cobrix_spark_code(schema),
        }

    def _generate_copybook_string(self, schema: CopybookSchema) -> str:
        """Regenerate copybook string from parsed schema."""
        lines = []

        def format_field(f: CopybookField, indent: int = 0):
            line = f"{' ' * indent}{f.level:02d}  {f.name}"

            if f.redefines:
                line += f" REDEFINES {f.redefines}"

            if f.pic_clause:
                pic = f.pic_clause
                if f.signed and not pic.startswith('S'):
                    pic = 'S' + pic
                line += f" PIC {pic}"

                if f.data_type == DataType.PACKED_DECIMAL:
                    line += " COMP-3"
                elif f.data_type == DataType.BINARY:
                    line += " COMP"

            if f.occurs > 1:
                line += f" OCCURS {f.occurs} TIMES"

            line += "."
            lines.append(line)

            for child in f.children:
                format_field(child, indent + 4)

        for field in schema.fields:
            format_field(field)

        return "\n".join(lines)

    def _generate_cobrix_spark_code(self, schema: CopybookSchema) -> str:
        """Generate Spark code for reading EBCDIC with Cobrix."""
        return f'''
# Read EBCDIC mainframe file with Cobrix
df = spark.read \\
    .format("cobol") \\
    .option("copybook_contents", """
{self._generate_copybook_string(schema)}
""") \\
    .option("encoding", "cp037") \\
    .option("record_format", "F") \\
    .option("record_length", {schema.record_length}) \\
    .option("schema_retention_policy", "collapse_root") \\
    .option("generate_record_id", True) \\
    .option("string_trimming_policy", "both") \\
    .load("gs://your-bucket/mainframe-data/")

# Show schema
df.printSchema()

# Preview data
df.show(10, truncate=False)
'''

    def to_json(self, schema: CopybookSchema) -> str:
        """Export schema as JSON for metadata storage."""
        return json.dumps(schema.to_dict(), indent=2)

    def to_template_config(self, schema: CopybookSchema) -> Dict[str, Any]:
        """
        Generate configuration for the file_source template.

        Returns dict suitable for Jinja2 template variables.
        """
        return {
            'source_format': 'ebcdic',
            'record_length': schema.record_length,
            'copybook': {
                'name': schema.name,
                'fields': schema.to_flat_fields(),
            },
            'encoding': 'cp037',
            'use_cobrix': True,
        }


# =============================================================================
# Utility Functions
# =============================================================================

def parse_copybook_from_gcs(bucket: str, blob_path: str) -> CopybookSchema:
    """
    Parse copybook from GCS.

    Args:
        bucket: GCS bucket name
        blob_path: Path to copybook file in bucket

    Returns:
        CopybookSchema
    """
    from google.cloud import storage

    client = storage.Client()
    bucket_obj = client.bucket(bucket)
    blob = bucket_obj.blob(blob_path)
    content = blob.download_as_string().decode('utf-8')

    parser = CopybookParser()
    return parser.parse(content, name=Path(blob_path).stem)


def validate_ebcdic_file(data_path: str, schema: CopybookSchema) -> Dict[str, Any]:
    """
    Validate EBCDIC file against copybook schema.

    Returns validation results.
    """
    from google.cloud import storage
    import struct

    # Parse GCS path
    if data_path.startswith('gs://'):
        parts = data_path[5:].split('/', 1)
        bucket_name = parts[0]
        blob_path = parts[1] if len(parts) > 1 else ''

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        # Get file size
        blob.reload()
        file_size = blob.size
    else:
        file_size = Path(data_path).stat().st_size

    expected_records = file_size // schema.record_length
    remainder = file_size % schema.record_length

    return {
        'file_path': data_path,
        'file_size_bytes': file_size,
        'record_length': schema.record_length,
        'expected_records': expected_records,
        'remainder_bytes': remainder,
        'valid': remainder == 0,
        'error': None if remainder == 0 else f"File size not divisible by record length. Remainder: {remainder} bytes",
    }


# =============================================================================
# Example Copybook
# =============================================================================

EXAMPLE_COPYBOOK = """
       01  CUSTOMER-RECORD.
           05  CUSTOMER-ID          PIC 9(10).
           05  CUSTOMER-NAME        PIC X(50).
           05  CUSTOMER-ADDRESS.
               10  STREET           PIC X(30).
               10  CITY             PIC X(20).
               10  STATE            PIC XX.
               10  ZIP-CODE         PIC 9(5).
           05  ACCOUNT-BALANCE      PIC S9(9)V99 COMP-3.
           05  LAST-ACTIVITY-DATE   PIC 9(8).
           05  STATUS-CODE          PIC X.
           05  FILLER               PIC X(10).
"""


if __name__ == "__main__":
    # Demo parsing
    parser = CopybookParser()
    schema = parser.parse(EXAMPLE_COPYBOOK, name="customer_record")

    print("=== Parsed Schema ===")
    print(json.dumps(schema.to_dict(), indent=2))

    print("\n=== Flat Fields ===")
    for field in schema.to_flat_fields():
        print(f"  {field['name']}: {field['type']} @ {field['start']} ({field['length']} bytes)")

    print(f"\n=== Record Length: {schema.record_length} bytes ===")

    print("\n=== Spark Schema ===")
    print(parser.to_spark_schema(schema))

    print("\n=== Cobrix Config ===")
    config = parser.to_cobrix_config(schema)
    print(config['spark_code'])
