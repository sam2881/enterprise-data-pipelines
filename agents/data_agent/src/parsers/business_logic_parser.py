"""
Business Logic Parser - Convert diverse inputs to executable PySpark.

WHY: Business logic comes in many forms:
- SQL queries from analysts
- DTSX packages from SSIS migrations
- Natural language from business users
- Column mapping spreadsheets

HOW: Each parser type extracts intent and generates PySpark:
1. Parse input format
2. Extract transformations
3. Generate PySpark code
4. Validate syntax
5. Return executable code

References:
- Uber Sparkle Framework: Config-driven ETL
- dbt for SQL transformations
"""

import json
import re
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import structlog

logger = structlog.get_logger()


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class SourceTable:
    """Reference to a source table."""
    table_name: str
    alias: Optional[str] = None
    schema: Optional[str] = None
    zone: str = "bronze"  # landing, bronze, silver, gold


@dataclass
class JoinCondition:
    """Join condition between tables."""
    left_column: str
    right_column: str
    join_type: str = "inner"  # inner, left, right, full, cross
    left_table: Optional[str] = None
    right_table: Optional[str] = None


@dataclass
class Aggregation:
    """Aggregation operation."""
    source_column: str
    function: str  # SUM, COUNT, AVG, MAX, MIN, COLLECT_LIST, etc.
    alias: str
    distinct: bool = False


@dataclass
class ColumnTransform:
    """Column transformation."""
    source_columns: List[str]
    target_column: str
    expression: str
    transform_type: str = "expression"  # expression, rename, cast, constant


@dataclass
class FilterCondition:
    """Filter/WHERE condition."""
    expression: str
    parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ParsedLogic:
    """Complete parsed business logic."""
    source_tables: List[SourceTable]
    joins: List[JoinCondition]
    filters: List[FilterCondition]
    transformations: List[ColumnTransform]
    aggregations: List[Aggregation]
    group_by_columns: List[str]
    order_by_columns: List[Tuple[str, str]]  # (column, asc/desc)
    distinct: bool = False
    limit: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "source_tables": [
                {"table": t.table_name, "alias": t.alias, "zone": t.zone}
                for t in self.source_tables
            ],
            "joins": [
                {
                    "left": j.left_column,
                    "right": j.right_column,
                    "type": j.join_type,
                }
                for j in self.joins
            ],
            "filters": [f.expression for f in self.filters],
            "transformations": [
                {
                    "source": t.source_columns,
                    "target": t.target_column,
                    "expression": t.expression,
                }
                for t in self.transformations
            ],
            "aggregations": [
                {"column": a.source_column, "function": a.function, "alias": a.alias}
                for a in self.aggregations
            ],
            "group_by": self.group_by_columns,
        }


# =============================================================================
# Abstract Parser
# =============================================================================


class BusinessLogicParser(ABC):
    """Abstract base class for business logic parsers."""

    def __init__(self):
        self.logger = logger.bind(parser=self.__class__.__name__)

    @abstractmethod
    def parse(self, input_data: Dict[str, Any]) -> ParsedLogic:
        """Parse input and return structured logic."""
        pass

    @abstractmethod
    def generate_pyspark(self, parsed: ParsedLogic) -> str:
        """Generate PySpark code from parsed logic."""
        pass

    def validate_syntax(self, code: str) -> Tuple[bool, List[str]]:
        """Validate generated PySpark code syntax."""
        errors = []
        try:
            compile(code, "<string>", "exec")
        except SyntaxError as e:
            errors.append(f"Syntax error at line {e.lineno}: {e.msg}")
        return len(errors) == 0, errors


# =============================================================================
# SQL Parser
# =============================================================================


class SQLParser(BusinessLogicParser):
    """
    Parse SQL queries and generate PySpark DataFrame code.

    Supports:
    - SELECT with expressions
    - JOINs (INNER, LEFT, RIGHT, FULL)
    - WHERE filters
    - GROUP BY / HAVING
    - ORDER BY
    - DISTINCT
    - Common functions

    Limitations:
    - No subqueries (use CTEs instead)
    - No window functions (use Spark SQL for complex queries)
    """

    # SQL function to PySpark function mapping
    FUNCTION_MAP = {
        "COALESCE": "F.coalesce",
        "IFNULL": "F.coalesce",
        "NVL": "F.coalesce",
        "CONCAT": "F.concat",
        "CONCAT_WS": "F.concat_ws",
        "UPPER": "F.upper",
        "LOWER": "F.lower",
        "TRIM": "F.trim",
        "LTRIM": "F.ltrim",
        "RTRIM": "F.rtrim",
        "LENGTH": "F.length",
        "SUBSTRING": "F.substring",
        "SUBSTR": "F.substring",
        "REPLACE": "F.regexp_replace",
        "CAST": "F.col({}).cast",
        "TO_DATE": "F.to_date",
        "TO_TIMESTAMP": "F.to_timestamp",
        "DATE_FORMAT": "F.date_format",
        "DATEDIFF": "F.datediff",
        "DATE_ADD": "F.date_add",
        "DATE_SUB": "F.date_sub",
        "YEAR": "F.year",
        "MONTH": "F.month",
        "DAY": "F.dayofmonth",
        "HOUR": "F.hour",
        "MINUTE": "F.minute",
        "ROUND": "F.round",
        "FLOOR": "F.floor",
        "CEIL": "F.ceil",
        "ABS": "F.abs",
        "SUM": "F.sum",
        "COUNT": "F.count",
        "AVG": "F.avg",
        "MAX": "F.max",
        "MIN": "F.min",
        "COUNT_DISTINCT": "F.countDistinct",
    }

    def parse(self, input_data: Dict[str, Any]) -> ParsedLogic:
        """Parse SQL query into structured logic."""
        sql = input_data.get("raw_sql", "")
        if not sql:
            raise ValueError("No SQL provided")

        self.logger.info("Parsing SQL", sql_length=len(sql))

        # Normalize SQL
        sql = self._normalize_sql(sql)

        # Extract components
        source_tables = self._extract_tables(sql)
        joins = self._extract_joins(sql)
        filters = self._extract_filters(sql)
        transformations = self._extract_select(sql)
        aggregations, group_by = self._extract_aggregations(sql)
        order_by = self._extract_order_by(sql)
        distinct = "SELECT DISTINCT" in sql.upper()
        limit = self._extract_limit(sql)

        return ParsedLogic(
            source_tables=source_tables,
            joins=joins,
            filters=filters,
            transformations=transformations,
            aggregations=aggregations,
            group_by_columns=group_by,
            order_by_columns=order_by,
            distinct=distinct,
            limit=limit,
        )

    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL for parsing."""
        # Remove comments
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        # Normalize whitespace
        sql = ' '.join(sql.split())
        return sql

    def _extract_tables(self, sql: str) -> List[SourceTable]:
        """Extract table references from FROM clause."""
        tables = []

        # Match FROM table [alias]
        from_match = re.search(
            r'FROM\s+(\w+(?:\.\w+)?)\s*(?:AS\s+)?(\w+)?',
            sql,
            re.IGNORECASE
        )
        if from_match:
            table_name = from_match.group(1)
            alias = from_match.group(2)
            # Determine zone from table name
            zone = "bronze"
            if table_name.startswith(("bronze.", "raw.")):
                zone = "bronze"
            elif table_name.startswith("silver."):
                zone = "silver"
            elif table_name.startswith("gold."):
                zone = "gold"

            tables.append(SourceTable(
                table_name=table_name,
                alias=alias,
                zone=zone,
            ))

        # Match JOIN tables
        join_pattern = r'(?:INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN\s+(\w+(?:\.\w+)?)\s*(?:AS\s+)?(\w+)?'
        for match in re.finditer(join_pattern, sql, re.IGNORECASE):
            tables.append(SourceTable(
                table_name=match.group(1),
                alias=match.group(2),
            ))

        return tables

    def _extract_joins(self, sql: str) -> List[JoinCondition]:
        """Extract JOIN conditions."""
        joins = []

        # Match JOIN ... ON conditions
        join_pattern = r'(INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN\s+\w+(?:\.\w+)?\s*(?:AS\s+)?(\w+)?\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)'
        for match in re.finditer(join_pattern, sql, re.IGNORECASE):
            join_type = (match.group(1) or "inner").lower()
            left_col = match.group(3)
            right_col = match.group(4)

            joins.append(JoinCondition(
                left_column=left_col,
                right_column=right_col,
                join_type=join_type,
            ))

        return joins

    def _extract_filters(self, sql: str) -> List[FilterCondition]:
        """Extract WHERE conditions."""
        filters = []

        # Match WHERE clause
        where_match = re.search(
            r'WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)',
            sql,
            re.IGNORECASE | re.DOTALL
        )
        if where_match:
            where_clause = where_match.group(1).strip()
            # Split by AND (simplified)
            conditions = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)
            for cond in conditions:
                cond = cond.strip()
                if cond:
                    filters.append(FilterCondition(expression=cond))

        return filters

    def _extract_select(self, sql: str) -> List[ColumnTransform]:
        """Extract SELECT columns and expressions."""
        transforms = []

        # Match SELECT clause
        select_match = re.search(
            r'SELECT\s+(?:DISTINCT\s+)?(.+?)\s+FROM',
            sql,
            re.IGNORECASE | re.DOTALL
        )
        if not select_match:
            return transforms

        select_clause = select_match.group(1)

        # Split by comma (handling nested functions)
        columns = self._split_select_columns(select_clause)

        for col_expr in columns:
            col_expr = col_expr.strip()
            if not col_expr or col_expr == "*":
                continue

            # Check for alias
            alias_match = re.search(r'(.+?)\s+(?:AS\s+)?(\w+)$', col_expr, re.IGNORECASE)
            if alias_match:
                expression = alias_match.group(1).strip()
                alias = alias_match.group(2)
            else:
                expression = col_expr
                # Use column name as alias
                alias = re.sub(r'[^\w]', '_', col_expr.split('.')[-1])

            # Determine source columns
            source_cols = re.findall(r'(\w+\.\w+|\w+)', expression)

            transforms.append(ColumnTransform(
                source_columns=source_cols,
                target_column=alias,
                expression=expression,
            ))

        return transforms

    def _split_select_columns(self, select_clause: str) -> List[str]:
        """Split SELECT columns handling nested parentheses."""
        columns = []
        current = ""
        depth = 0

        for char in select_clause:
            if char == '(':
                depth += 1
                current += char
            elif char == ')':
                depth -= 1
                current += char
            elif char == ',' and depth == 0:
                columns.append(current.strip())
                current = ""
            else:
                current += char

        if current.strip():
            columns.append(current.strip())

        return columns

    def _extract_aggregations(self, sql: str) -> Tuple[List[Aggregation], List[str]]:
        """Extract aggregate functions and GROUP BY columns."""
        aggregations = []
        group_by = []

        # Find aggregate functions in SELECT
        agg_pattern = r'(SUM|COUNT|AVG|MAX|MIN)\s*\(\s*(DISTINCT\s+)?(\w+(?:\.\w+)?)\s*\)\s*(?:AS\s+)?(\w+)?'
        for match in re.finditer(agg_pattern, sql, re.IGNORECASE):
            func = match.group(1).upper()
            distinct = match.group(2) is not None
            column = match.group(3)
            alias = match.group(4) or f"{func.lower()}_{column.replace('.', '_')}"

            aggregations.append(Aggregation(
                source_column=column,
                function=func,
                alias=alias,
                distinct=distinct,
            ))

        # Extract GROUP BY
        group_match = re.search(
            r'GROUP BY\s+(.+?)(?:HAVING|ORDER BY|LIMIT|$)',
            sql,
            re.IGNORECASE
        )
        if group_match:
            group_clause = group_match.group(1)
            group_by = [col.strip() for col in group_clause.split(',')]

        return aggregations, group_by

    def _extract_order_by(self, sql: str) -> List[Tuple[str, str]]:
        """Extract ORDER BY columns."""
        order_by = []

        order_match = re.search(
            r'ORDER BY\s+(.+?)(?:LIMIT|$)',
            sql,
            re.IGNORECASE
        )
        if order_match:
            order_clause = order_match.group(1)
            for item in order_clause.split(','):
                item = item.strip()
                if ' DESC' in item.upper():
                    col = item.upper().replace(' DESC', '').strip()
                    order_by.append((col, 'desc'))
                else:
                    col = item.upper().replace(' ASC', '').strip()
                    order_by.append((col, 'asc'))

        return order_by

    def _extract_limit(self, sql: str) -> Optional[int]:
        """Extract LIMIT clause."""
        limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        if limit_match:
            return int(limit_match.group(1))
        return None

    def generate_pyspark(self, parsed: ParsedLogic) -> str:
        """Generate PySpark DataFrame code from parsed SQL logic."""
        lines = ["# Business Logic: SQL Query Transform", ""]

        # Read source tables
        for i, table in enumerate(parsed.source_tables):
            var_name = table.alias or f"df_{i}"
            lines.append(f'# Read source: {table.table_name}')
            lines.append(f'{var_name} = spark.read.table("{table.table_name}")')
            lines.append("")

        # Set main DataFrame
        if parsed.source_tables:
            main_df = parsed.source_tables[0].alias or "df_0"
            lines.append(f"df = {main_df}")

        # Apply joins
        for join in parsed.joins:
            lines.append(f'# Join on {join.left_column} = {join.right_column}')
            right_alias = join.right_column.split('.')[0]
            lines.append(f'df = df.join(')
            lines.append(f'    {right_alias},')
            lines.append(f'    on=F.col("{join.left_column}") == F.col("{join.right_column}"),')
            lines.append(f'    how="{join.join_type}"')
            lines.append(f')')
            lines.append("")

        # Apply filters
        if parsed.filters:
            lines.append("# Apply filters")
            for f in parsed.filters:
                # Convert SQL filter to Spark
                spark_filter = self._convert_filter(f.expression)
                lines.append(f'df = df.filter({spark_filter})')
            lines.append("")

        # Apply transformations/select
        if parsed.transformations and not parsed.aggregations:
            lines.append("# Select and transform columns")
            select_exprs = []
            for t in parsed.transformations:
                spark_expr = self._convert_expression(t.expression)
                select_exprs.append(f'    {spark_expr}.alias("{t.target_column}")')
            lines.append("df = df.select(")
            lines.append(",\n".join(select_exprs))
            lines.append(")")
            lines.append("")

        # Apply aggregations
        if parsed.aggregations:
            lines.append("# Group and aggregate")
            if parsed.group_by_columns:
                group_cols = ', '.join([f'"{c}"' for c in parsed.group_by_columns])
                lines.append(f"df = df.groupBy({group_cols})")
            else:
                lines.append("df = df.groupBy()")

            agg_exprs = []
            for agg in parsed.aggregations:
                func = self.FUNCTION_MAP.get(agg.function, f"F.{agg.function.lower()}")
                if agg.distinct and agg.function == "COUNT":
                    expr = f'F.countDistinct("{agg.source_column}").alias("{agg.alias}")'
                else:
                    expr = f'{func}("{agg.source_column}").alias("{agg.alias}")'
                agg_exprs.append(f'    {expr}')

            lines.append(".agg(")
            lines.append(",\n".join(agg_exprs))
            lines.append(")")
            lines.append("")

        # Apply distinct
        if parsed.distinct:
            lines.append("df = df.distinct()")
            lines.append("")

        # Apply order by
        if parsed.order_by_columns:
            lines.append("# Sort results")
            order_exprs = []
            for col, direction in parsed.order_by_columns:
                if direction == "desc":
                    order_exprs.append(f'F.col("{col}").desc()')
                else:
                    order_exprs.append(f'F.col("{col}").asc()')
            lines.append(f'df = df.orderBy({", ".join(order_exprs)})')
            lines.append("")

        # Apply limit
        if parsed.limit:
            lines.append(f"df = df.limit({parsed.limit})")
            lines.append("")

        return "\n".join(lines)

    def _convert_filter(self, sql_filter: str) -> str:
        """Convert SQL filter to PySpark expression."""
        # Simple conversion - for complex cases, use F.expr()
        spark_filter = sql_filter

        # Convert common patterns
        spark_filter = re.sub(r"(\w+)\s*=\s*'([^']*)'", r'(F.col("\1") == "\2")', spark_filter)
        spark_filter = re.sub(r"(\w+)\s*=\s*(\d+)", r'(F.col("\1") == \2)', spark_filter)
        spark_filter = re.sub(r"(\w+)\s+IS\s+NULL", r'F.col("\1").isNull()', spark_filter, flags=re.IGNORECASE)
        spark_filter = re.sub(r"(\w+)\s+IS\s+NOT\s+NULL", r'F.col("\1").isNotNull()', spark_filter, flags=re.IGNORECASE)
        spark_filter = re.sub(r"(\w+)\s+IN\s+\(([^)]+)\)", r'F.col("\1").isin(\2)', spark_filter, flags=re.IGNORECASE)
        spark_filter = re.sub(r"(\w+)\s+LIKE\s+'([^']*)'", r'F.col("\1").like("\2")', spark_filter, flags=re.IGNORECASE)

        # If conversion failed, use F.expr
        if spark_filter == sql_filter:
            return f'F.expr("{sql_filter}")'

        return spark_filter

    def _convert_expression(self, sql_expr: str) -> str:
        """Convert SQL expression to PySpark."""
        # Simple column reference
        if re.match(r'^[\w.]+$', sql_expr):
            return f'F.col("{sql_expr}")'

        # Function calls
        for sql_func, spark_func in self.FUNCTION_MAP.items():
            pattern = rf'{sql_func}\s*\(([^)]+)\)'
            if re.search(pattern, sql_expr, re.IGNORECASE):
                # For complex expressions, use F.expr
                return f'F.expr("{sql_expr}")'

        # Default to F.expr
        return f'F.expr("{sql_expr}")'


# =============================================================================
# DTSX (SSIS) Parser
# =============================================================================


class DTSXParser(BusinessLogicParser):
    """
    Parse SSIS DTSX packages and extract transformation logic.

    Extracts:
    - Data Flow tasks
    - Source/Destination components
    - Derived Column transforms
    - Lookup transforms
    - Aggregate transforms
    - Conditional Split logic

    WHY: Many enterprises have legacy SSIS packages that need migration.
    """

    def parse(self, input_data: Dict[str, Any]) -> ParsedLogic:
        """Parse DTSX package XML."""
        dtsx_content = input_data.get("dtsx_content", "")
        dtsx_path = input_data.get("dtsx_path", "")
        dataflow_name = input_data.get("dataflow_name", "")

        if dtsx_path and not dtsx_content:
            # Would read from GCS/S3
            self.logger.info("Reading DTSX from path", path=dtsx_path)
            # dtsx_content = read_from_storage(dtsx_path)

        if not dtsx_content:
            raise ValueError("No DTSX content provided")

        self.logger.info("Parsing DTSX package")

        # Parse XML
        root = ET.fromstring(dtsx_content)

        # Find Data Flow task
        dataflow = self._find_dataflow(root, dataflow_name)
        if not dataflow:
            raise ValueError(f"Data Flow '{dataflow_name}' not found in package")

        # Extract components
        source_tables = self._extract_sources(dataflow)
        transforms = self._extract_derived_columns(dataflow)
        joins = self._extract_lookups(dataflow)
        aggregations, group_by = self._extract_aggregations(dataflow)

        return ParsedLogic(
            source_tables=source_tables,
            joins=joins,
            filters=[],
            transformations=transforms,
            aggregations=aggregations,
            group_by_columns=group_by,
            order_by_columns=[],
        )

    def _find_dataflow(self, root: ET.Element, name: str) -> Optional[ET.Element]:
        """Find Data Flow task by name."""
        # SSIS namespace
        ns = {
            'DTS': 'www.microsoft.com/SqlServer/Dts',
            'SSIS': 'www.microsoft.com/SqlServer/SSIS',
        }

        # Search for data flow tasks
        for elem in root.iter():
            if 'DataFlow' in elem.tag or 'Pipeline' in elem.tag:
                task_name = elem.get('{www.microsoft.com/SqlServer/Dts}ObjectName', '')
                if not name or name in task_name:
                    return elem

        return None

    def _extract_sources(self, dataflow: ET.Element) -> List[SourceTable]:
        """Extract source components."""
        sources = []

        for elem in dataflow.iter():
            if 'OleDbSource' in elem.tag or 'FlatFileSource' in elem.tag:
                # Get table/file name
                for prop in elem.iter():
                    if 'SqlCommand' in prop.tag or 'TableOrViewName' in prop.tag:
                        table_name = prop.text or ""
                        if table_name:
                            sources.append(SourceTable(table_name=table_name))

        return sources

    def _extract_derived_columns(self, dataflow: ET.Element) -> List[ColumnTransform]:
        """Extract Derived Column transforms."""
        transforms = []

        for elem in dataflow.iter():
            if 'DerivedColumn' in elem.tag:
                for col in elem.iter():
                    # Get column name and expression
                    name = col.get('name', '')
                    expr = col.get('expression', '')
                    if name and expr:
                        transforms.append(ColumnTransform(
                            source_columns=[],  # Would need to parse expression
                            target_column=name,
                            expression=self._convert_ssis_expression(expr),
                        ))

        return transforms

    def _extract_lookups(self, dataflow: ET.Element) -> List[JoinCondition]:
        """Extract Lookup transforms as joins."""
        joins = []

        for elem in dataflow.iter():
            if 'Lookup' in elem.tag:
                # Get reference table and join columns
                ref_table = ""
                left_col = ""
                right_col = ""

                for prop in elem.iter():
                    if 'SqlCommand' in prop.tag:
                        # Extract table from SQL
                        sql = prop.text or ""
                        match = re.search(r'FROM\s+(\w+)', sql, re.IGNORECASE)
                        if match:
                            ref_table = match.group(1)

                if ref_table and left_col and right_col:
                    joins.append(JoinCondition(
                        left_column=left_col,
                        right_column=right_col,
                        join_type="left",
                    ))

        return joins

    def _extract_aggregations(self, dataflow: ET.Element) -> Tuple[List[Aggregation], List[str]]:
        """Extract Aggregate transforms."""
        aggregations = []
        group_by = []

        for elem in dataflow.iter():
            if 'Aggregate' in elem.tag:
                for col in elem.iter():
                    agg_type = col.get('aggregationType', '')
                    source_col = col.get('sourceColumnID', '')
                    output_col = col.get('name', '')

                    if agg_type == 'GroupBy':
                        group_by.append(source_col)
                    elif agg_type:
                        aggregations.append(Aggregation(
                            source_column=source_col,
                            function=agg_type.upper(),
                            alias=output_col,
                        ))

        return aggregations, group_by

    def _convert_ssis_expression(self, ssis_expr: str) -> str:
        """Convert SSIS expression to Spark SQL."""
        spark_expr = ssis_expr

        # SSIS to Spark conversions
        conversions = [
            (r'\(DT_STR,\s*\d+,\s*\d+\)', 'CAST({} AS STRING)'),
            (r'\(DT_I4\)', 'CAST({} AS INT)'),
            (r'\(DT_DECIMAL,\s*\d+,\s*\d+\)', 'CAST({} AS DECIMAL)'),
            (r'ISNULL\((\w+)\)', r'ISNULL(\1)'),
            (r'REPLACE\(', 'REPLACE('),
            (r'SUBSTRING\(', 'SUBSTRING('),
            (r'UPPER\(', 'UPPER('),
            (r'LOWER\(', 'LOWER('),
            (r'TRIM\(', 'TRIM('),
            (r'LEN\(', 'LENGTH('),
            (r'GETDATE\(\)', 'current_timestamp()'),
        ]

        for pattern, replacement in conversions:
            spark_expr = re.sub(pattern, replacement, spark_expr, flags=re.IGNORECASE)

        return spark_expr

    def generate_pyspark(self, parsed: ParsedLogic) -> str:
        """Generate PySpark from parsed DTSX logic."""
        lines = ["# Business Logic: DTSX Migration", ""]

        # Read sources
        for i, table in enumerate(parsed.source_tables):
            lines.append(f'df_{i} = spark.read.table("{table.table_name}")')

        if parsed.source_tables:
            lines.append("df = df_0")
            lines.append("")

        # Apply joins
        for i, join in enumerate(parsed.joins):
            lines.append(f"# Lookup join")
            lines.append(f"df = df.join(lookup_df_{i}, "
                        f'F.col("{join.left_column}") == F.col("{join.right_column}"), '
                        f'"left")')
            lines.append("")

        # Apply transformations
        if parsed.transformations:
            lines.append("# Derived columns")
            for t in parsed.transformations:
                lines.append(f'df = df.withColumn("{t.target_column}", F.expr("{t.expression}"))')
            lines.append("")

        # Apply aggregations
        if parsed.aggregations:
            lines.append("# Aggregations")
            group_cols = ', '.join([f'"{c}"' for c in parsed.group_by_columns])
            lines.append(f"df = df.groupBy({group_cols}).agg(")
            for agg in parsed.aggregations:
                lines.append(f'    F.{agg.function.lower()}("{agg.source_column}").alias("{agg.alias}"),')
            lines.append(")")
            lines.append("")

        return "\n".join(lines)


# =============================================================================
# Natural Language Parser (LLM-powered)
# =============================================================================


class NaturalLanguageParser(BusinessLogicParser):
    """
    Parse natural language descriptions and generate PySpark using LLM.

    Enables business users to describe transformations in plain English.

    Example inputs:
    - "Calculate total sales by region for completed orders"
    - "Join customers with orders where order_date is in the last 30 days"
    - "Remove duplicates based on customer_id, keeping the latest record"
    """

    def __init__(self, llm_client: Any = None, model: str = "claude-3-opus"):
        super().__init__()
        self.llm_client = llm_client
        self.model = model

    def parse(self, input_data: Dict[str, Any]) -> ParsedLogic:
        """Parse natural language description."""
        description = input_data.get("description", "")
        source_tables = input_data.get("source_tables", [])
        target_columns = input_data.get("target_columns", [])
        schema_info = input_data.get("schema_info", {})

        if not description:
            raise ValueError("No description provided")

        self.logger.info("Parsing natural language", description=description[:100])

        # Build prompt for LLM
        prompt = self._build_prompt(description, source_tables, target_columns, schema_info)

        # Call LLM
        if self.llm_client:
            response = self._call_llm(prompt)
            return self._parse_llm_response(response)

        # Fallback to simple pattern matching
        return self._simple_parse(description, source_tables)

    def _build_prompt(
        self,
        description: str,
        source_tables: List[Dict[str, str]],
        target_columns: List[str],
        schema_info: Dict[str, Any],
    ) -> str:
        """Build LLM prompt for parsing."""
        tables_str = json.dumps(source_tables, indent=2) if source_tables else "Not specified"
        columns_str = ", ".join(target_columns) if target_columns else "Not specified"
        schema_str = json.dumps(schema_info, indent=2) if schema_info else "Not specified"

        return f"""You are a data engineering expert. Convert this natural language description to a structured data transformation specification.

## Description
{description}

## Available Source Tables
{tables_str}

## Expected Target Columns
{columns_str}

## Schema Information
{schema_str}

## Task
Analyze the description and return a JSON object with:
- source_tables: List of tables to read [{{"table": "...", "alias": "...", "zone": "bronze/silver/gold"}}]
- joins: List of join conditions [{{"left": "...", "right": "...", "type": "inner/left/right"}}]
- filters: List of filter expressions ["..."]
- transformations: List of column transforms [{{"source": [...], "target": "...", "expression": "..."}}]
- aggregations: List of aggregations [{{"column": "...", "function": "SUM/COUNT/AVG/etc", "alias": "..."}}]
- group_by: List of group by columns [...]
- order_by: List of order by columns [["column", "asc/desc"]]

Respond only with valid JSON.
"""

    def _call_llm(self, prompt: str) -> str:
        """Call LLM API."""
        try:
            # Anthropic Claude
            if hasattr(self.llm_client, 'messages'):
                response = self.llm_client.messages.create(
                    model=self.model,
                    max_tokens=2000,
                    messages=[{"role": "user", "content": prompt}],
                )
                return response.content[0].text
            # OpenAI
            elif hasattr(self.llm_client, 'chat'):
                response = self.llm_client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                )
                return response.choices[0].message.content
        except Exception as e:
            self.logger.error("LLM call failed", error=str(e))
            return "{}"

        return "{}"

    def _parse_llm_response(self, response: str) -> ParsedLogic:
        """Parse LLM response JSON into ParsedLogic."""
        try:
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if not json_match:
                raise ValueError("No JSON found in response")

            data = json.loads(json_match.group())

            return ParsedLogic(
                source_tables=[
                    SourceTable(
                        table_name=t.get("table", ""),
                        alias=t.get("alias"),
                        zone=t.get("zone", "bronze"),
                    )
                    for t in data.get("source_tables", [])
                ],
                joins=[
                    JoinCondition(
                        left_column=j.get("left", ""),
                        right_column=j.get("right", ""),
                        join_type=j.get("type", "inner"),
                    )
                    for j in data.get("joins", [])
                ],
                filters=[
                    FilterCondition(expression=f)
                    for f in data.get("filters", [])
                ],
                transformations=[
                    ColumnTransform(
                        source_columns=t.get("source", []),
                        target_column=t.get("target", ""),
                        expression=t.get("expression", ""),
                    )
                    for t in data.get("transformations", [])
                ],
                aggregations=[
                    Aggregation(
                        source_column=a.get("column", ""),
                        function=a.get("function", "COUNT"),
                        alias=a.get("alias", ""),
                    )
                    for a in data.get("aggregations", [])
                ],
                group_by_columns=data.get("group_by", []),
                order_by_columns=[
                    tuple(o) if isinstance(o, list) else (o, "asc")
                    for o in data.get("order_by", [])
                ],
            )
        except (json.JSONDecodeError, KeyError) as e:
            self.logger.error("Failed to parse LLM response", error=str(e))
            return ParsedLogic(
                source_tables=[],
                joins=[],
                filters=[],
                transformations=[],
                aggregations=[],
                group_by_columns=[],
                order_by_columns=[],
            )

    def _simple_parse(
        self,
        description: str,
        source_tables: List[Dict[str, str]]
    ) -> ParsedLogic:
        """Simple pattern-based parsing fallback."""
        description_lower = description.lower()

        # Detect aggregations
        aggregations = []
        agg_patterns = [
            (r'total\s+(\w+)', 'SUM'),
            (r'sum\s+(?:of\s+)?(\w+)', 'SUM'),
            (r'count\s+(?:of\s+)?(\w+)', 'COUNT'),
            (r'average\s+(?:of\s+)?(\w+)', 'AVG'),
            (r'maximum\s+(?:of\s+)?(\w+)', 'MAX'),
            (r'minimum\s+(?:of\s+)?(\w+)', 'MIN'),
        ]
        for pattern, func in agg_patterns:
            match = re.search(pattern, description_lower)
            if match:
                col = match.group(1)
                aggregations.append(Aggregation(
                    source_column=col,
                    function=func,
                    alias=f"{func.lower()}_{col}",
                ))

        # Detect group by
        group_by = []
        group_match = re.search(r'by\s+(\w+(?:\s+and\s+\w+)*)', description_lower)
        if group_match:
            group_by = re.findall(r'\w+', group_match.group(1))

        return ParsedLogic(
            source_tables=[
                SourceTable(table_name=t.get("table", ""), alias=t.get("alias"))
                for t in source_tables
            ],
            joins=[],
            filters=[],
            transformations=[],
            aggregations=aggregations,
            group_by_columns=group_by,
            order_by_columns=[],
        )

    def generate_pyspark(self, parsed: ParsedLogic) -> str:
        """Generate PySpark from parsed natural language logic."""
        # Use SQL parser's generator since structure is the same
        sql_parser = SQLParser()
        return sql_parser.generate_pyspark(parsed)


# =============================================================================
# Column Mapping Parser
# =============================================================================


class ColumnMappingParser(BusinessLogicParser):
    """
    Parse simple column mapping specifications.

    Input format:
    {
        "mappings": [
            {"source": "old_name", "target": "new_name"},
            {"source": "col1", "target": "col2", "transform": "UPPER"},
            {"target": "full_name", "expression": "CONCAT(first_name, ' ', last_name)"}
        ]
    }
    """

    def parse(self, input_data: Dict[str, Any]) -> ParsedLogic:
        """Parse column mapping specification."""
        mappings = input_data.get("mappings", [])
        source_table = input_data.get("source_table", "")

        transforms = []
        for m in mappings:
            source = m.get("source", "")
            target = m.get("target", "")
            transform = m.get("transform", "")
            expression = m.get("expression", "")

            if expression:
                # Custom expression
                transforms.append(ColumnTransform(
                    source_columns=[],
                    target_column=target,
                    expression=expression,
                ))
            elif transform:
                # Simple transform function
                transforms.append(ColumnTransform(
                    source_columns=[source],
                    target_column=target,
                    expression=f"{transform}({source})",
                ))
            else:
                # Simple rename
                transforms.append(ColumnTransform(
                    source_columns=[source],
                    target_column=target,
                    expression=source,
                    transform_type="rename",
                ))

        return ParsedLogic(
            source_tables=[SourceTable(table_name=source_table)] if source_table else [],
            joins=[],
            filters=[],
            transformations=transforms,
            aggregations=[],
            group_by_columns=[],
            order_by_columns=[],
        )

    def generate_pyspark(self, parsed: ParsedLogic) -> str:
        """Generate PySpark from column mappings."""
        lines = ["# Business Logic: Column Mapping", ""]

        # Read source
        if parsed.source_tables:
            lines.append(f'df = spark.read.table("{parsed.source_tables[0].table_name}")')
            lines.append("")

        # Apply mappings
        for t in parsed.transformations:
            if t.transform_type == "rename":
                lines.append(f'df = df.withColumnRenamed("{t.source_columns[0]}", "{t.target_column}")')
            else:
                lines.append(f'df = df.withColumn("{t.target_column}", F.expr("{t.expression}"))')

        return "\n".join(lines)


# =============================================================================
# Parser Factory
# =============================================================================


class BusinessLogicParserFactory:
    """Factory for creating business logic parsers."""

    PARSERS = {
        "sql_query": SQLParser,
        "sql": SQLParser,
        "dtsx_extract": DTSXParser,
        "dtsx": DTSXParser,
        "ssis": DTSXParser,
        "natural_language": NaturalLanguageParser,
        "nl": NaturalLanguageParser,
        "column_mapping": ColumnMappingParser,
        "mapping": ColumnMappingParser,
    }

    @classmethod
    def create_parser(
        cls,
        logic_type: str,
        llm_client: Any = None,
    ) -> BusinessLogicParser:
        """Create parser for the given logic type."""
        parser_class = cls.PARSERS.get(logic_type.lower())
        if not parser_class:
            raise ValueError(f"Unknown logic type: {logic_type}")

        if parser_class == NaturalLanguageParser:
            return parser_class(llm_client=llm_client)

        return parser_class()

    @classmethod
    def list_supported_types(cls) -> List[str]:
        """Return list of supported logic types."""
        return list(cls.PARSERS.keys())
