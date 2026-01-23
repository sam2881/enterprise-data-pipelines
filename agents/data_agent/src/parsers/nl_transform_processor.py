"""
Natural Language Transform Processor - Convert NL to PySpark/SQL.

WHY: Enable non-technical users to define complex transformations,
     joins, and aggregations using natural language descriptions.

HOW: Use LLM to convert natural language to validated PySpark/SQL code.
     Implements:
     - Intent understanding
     - Schema-aware code generation
     - Validation and safety checks
     - Code optimization

References:
- AWS Enterprise NL2SQL: https://aws.amazon.com/blogs/machine-learning/enterprise-grade-natural-language-to-sql-generation-using-llms/
- LLM Code Generation Best Practices
"""

import json
import re
import ast
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

import structlog

logger = structlog.get_logger()


class TransformIntent(str, Enum):
    """Detected transformation intent."""
    FILTER = "filter"
    SELECT = "select"
    RENAME = "rename"
    DERIVE = "derive"
    AGGREGATE = "aggregate"
    JOIN = "join"
    WINDOW = "window"
    DEDUPLICATE = "deduplicate"
    SORT = "sort"
    UNION = "union"
    PIVOT = "pivot"
    UNPIVOT = "unpivot"
    SCD = "scd"
    COMPLEX = "complex"  # Multiple operations


@dataclass
class TransformRequest:
    """Natural language transformation request."""
    description: str
    source_schema: Dict[str, str]  # {"column_name": "data_type"}
    target_columns: Optional[List[str]] = None
    context: Optional[Dict[str, Any]] = None


@dataclass
class GeneratedCode:
    """Generated transformation code."""
    pyspark_code: str
    sql_code: Optional[str] = None
    intent: TransformIntent = TransformIntent.COMPLEX
    confidence: float = 0.0
    explanation: str = ""
    warnings: List[str] = field(default_factory=list)
    referenced_columns: List[str] = field(default_factory=list)


class NLTransformProcessor:
    """
    Convert natural language to PySpark/SQL transformations.

    Usage:
        processor = NLTransformProcessor(llm_client=anthropic_client)

        # Generate code from natural language
        result = await processor.generate_transform(
            description="Calculate running total of sales by customer ordered by date",
            source_schema={"customer_id": "string", "sales_amount": "decimal", "order_date": "date"}
        )

        print(result.pyspark_code)
    """

    # Common transformation patterns for rule-based fallback
    PATTERNS = {
        r"filter\s+(?:where\s+)?(.+)": TransformIntent.FILTER,
        r"select\s+(?:only\s+)?(.+)": TransformIntent.SELECT,
        r"rename\s+(.+)\s+to\s+(.+)": TransformIntent.RENAME,
        r"(?:calculate|compute|add)\s+(.+)": TransformIntent.DERIVE,
        r"(?:group\s+by|aggregate|sum|count|avg|average).+": TransformIntent.AGGREGATE,
        r"join\s+(.+)\s+(?:with|to|on)\s+(.+)": TransformIntent.JOIN,
        r"(?:running|cumulative|rolling|window).+": TransformIntent.WINDOW,
        r"(?:deduplicate|remove\s+duplicates|distinct).+": TransformIntent.DEDUPLICATE,
        r"(?:sort|order)\s+(?:by\s+)?(.+)": TransformIntent.SORT,
        r"(?:scd|slowly\s+changing|track\s+history).+": TransformIntent.SCD,
    }

    def __init__(
        self,
        llm_client: Any = None,
        model: str = "claude-3-opus",
        validate_code: bool = True,
    ):
        """
        Initialize NL transform processor.

        Args:
            llm_client: Anthropic/OpenAI client
            model: Model to use for code generation
            validate_code: Whether to validate generated code syntax
        """
        self.llm_client = llm_client
        self.model = model
        self.validate_code = validate_code
        self.logger = logger.bind(component="nl_transform_processor")

    async def generate_transform(self, request: TransformRequest) -> GeneratedCode:
        """
        Generate PySpark transformation code from natural language.

        Args:
            request: TransformRequest with description and schema

        Returns:
            GeneratedCode with PySpark and optionally SQL
        """
        self.logger.info("Generating transform", description=request.description[:100])

        # Detect intent
        intent = self._detect_intent(request.description)

        # Try rule-based generation first for simple cases
        if intent in [TransformIntent.FILTER, TransformIntent.SELECT, TransformIntent.SORT, TransformIntent.DEDUPLICATE]:
            rule_based = self._generate_rule_based(request, intent)
            if rule_based:
                return rule_based

        # Use LLM for complex transformations
        if self.llm_client:
            return await self._generate_with_llm(request, intent)

        # Fallback to template-based generation
        return self._generate_template_based(request, intent)

    def _detect_intent(self, description: str) -> TransformIntent:
        """Detect transformation intent from description."""
        description_lower = description.lower()

        for pattern, intent in self.PATTERNS.items():
            if re.search(pattern, description_lower):
                return intent

        return TransformIntent.COMPLEX

    def _generate_rule_based(self, request: TransformRequest, intent: TransformIntent) -> Optional[GeneratedCode]:
        """Generate code using rule-based patterns for simple cases."""
        description = request.description.lower()
        schema = request.source_schema

        if intent == TransformIntent.FILTER:
            # Extract filter condition
            match = re.search(r"(?:filter|where)\s+(.+)", description)
            if match:
                condition = self._convert_natural_condition(match.group(1), schema)
                code = f'df = df.filter("{condition}")'
                return GeneratedCode(
                    pyspark_code=code,
                    sql_code=f"SELECT * FROM table WHERE {condition}",
                    intent=intent,
                    confidence=0.8,
                    explanation=f"Filter records where {condition}",
                )

        elif intent == TransformIntent.SELECT:
            # Extract column names
            columns = self._extract_columns(description, schema)
            if columns:
                code = f'df = df.select({", ".join(f\'"{c}"\' for c in columns)})'
                return GeneratedCode(
                    pyspark_code=code,
                    sql_code=f"SELECT {', '.join(columns)} FROM table",
                    intent=intent,
                    confidence=0.85,
                    explanation=f"Select columns: {', '.join(columns)}",
                    referenced_columns=columns,
                )

        elif intent == TransformIntent.SORT:
            # Extract sort column and direction
            match = re.search(r"(?:sort|order)\s+(?:by\s+)?(\w+)(?:\s+(asc|desc))?", description)
            if match:
                column = match.group(1)
                direction = match.group(2) or 'asc'
                code = f'df = df.orderBy(F.col("{column}").{direction}())'
                return GeneratedCode(
                    pyspark_code=code,
                    sql_code=f"SELECT * FROM table ORDER BY {column} {direction.upper()}",
                    intent=intent,
                    confidence=0.9,
                    explanation=f"Sort by {column} {direction}",
                    referenced_columns=[column],
                )

        elif intent == TransformIntent.DEDUPLICATE:
            # Extract key columns
            match = re.search(r"(?:by|on|based on)\s+(.+)", description)
            if match:
                columns = [c.strip() for c in match.group(1).split(',')]
            else:
                columns = list(schema.keys())[:1]  # Default to first column

            code = f'df = df.dropDuplicates({columns})'
            return GeneratedCode(
                pyspark_code=code,
                sql_code=f"SELECT DISTINCT ON ({', '.join(columns)}) * FROM table",
                intent=intent,
                confidence=0.85,
                explanation=f"Remove duplicates based on: {', '.join(columns)}",
                referenced_columns=columns,
            )

        return None

    def _convert_natural_condition(self, condition: str, schema: Dict[str, str]) -> str:
        """Convert natural language condition to SQL-like condition."""
        # Common replacements
        replacements = [
            (r"is\s+not\s+null", "IS NOT NULL"),
            (r"is\s+null", "IS NULL"),
            (r"is\s+empty", "= ''"),
            (r"is\s+not\s+empty", "<> ''"),
            (r"equals?\s+", "= "),
            (r"greater\s+than\s+or\s+equal\s+to", ">="),
            (r"less\s+than\s+or\s+equal\s+to", "<="),
            (r"greater\s+than", ">"),
            (r"less\s+than", "<"),
            (r"not\s+equal\s+to", "<>"),
            (r"contains\s+'([^']+)'", r"LIKE '%\1%'"),
            (r"starts\s+with\s+'([^']+)'", r"LIKE '\1%'"),
            (r"ends\s+with\s+'([^']+)'", r"LIKE '%\1'"),
            (r"\band\b", "AND"),
            (r"\bor\b", "OR"),
        ]

        result = condition
        for pattern, replacement in replacements:
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

        return result

    def _extract_columns(self, description: str, schema: Dict[str, str]) -> List[str]:
        """Extract column names from description."""
        columns = []
        schema_lower = {k.lower(): k for k in schema.keys()}

        # Look for column names in description
        words = re.findall(r'\b\w+\b', description.lower())
        for word in words:
            if word in schema_lower:
                columns.append(schema_lower[word])

        return columns

    async def _generate_with_llm(self, request: TransformRequest, intent: TransformIntent) -> GeneratedCode:
        """Generate code using LLM."""
        prompt = self._build_llm_prompt(request, intent)

        try:
            response = await self.llm_client.messages.create(
                model=self.model,
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )
            response_text = response.content[0].text

            return self._parse_llm_response(response_text, request, intent)

        except Exception as e:
            self.logger.error("LLM generation failed", error=str(e))
            return self._generate_template_based(request, intent)

    def _build_llm_prompt(self, request: TransformRequest, intent: TransformIntent) -> str:
        """Build prompt for LLM code generation."""
        schema_str = json.dumps(request.source_schema, indent=2)

        return f"""You are an expert PySpark developer. Generate PySpark code for the following transformation.

## Source Schema:
```json
{schema_str}
```

## Transformation Request:
"{request.description}"

## Requirements:
1. Use PySpark DataFrame API with functions from pyspark.sql.functions as F
2. The input DataFrame is named 'df'
3. The output should also be 'df'
4. Include necessary imports
5. Handle null values appropriately
6. Add comments explaining the logic

## Output Format:
Provide the PySpark code in a ```python code block, followed by an equivalent SQL query in a ```sql code block.

Also provide:
- Confidence score (0.0-1.0)
- Brief explanation
- Any warnings about edge cases

Generate the code:
"""

    def _parse_llm_response(self, response: str, request: TransformRequest, intent: TransformIntent) -> GeneratedCode:
        """Parse LLM response and extract code."""
        # Extract Python code
        python_match = re.search(r'```python\n(.*?)```', response, re.DOTALL)
        pyspark_code = python_match.group(1).strip() if python_match else ""

        # Extract SQL code
        sql_match = re.search(r'```sql\n(.*?)```', response, re.DOTALL)
        sql_code = sql_match.group(1).strip() if sql_match else None

        # Extract confidence
        confidence_match = re.search(r'[Cc]onfidence[:\s]+(\d+\.?\d*)', response)
        confidence = float(confidence_match.group(1)) if confidence_match else 0.7

        # Extract explanation
        explanation = ""
        for line in response.split('\n'):
            if 'explanation' in line.lower() or 'this code' in line.lower():
                explanation = line.strip()
                break

        # Validate code if enabled
        warnings = []
        if self.validate_code:
            is_valid, validation_warnings = self._validate_pyspark(pyspark_code)
            warnings.extend(validation_warnings)
            if not is_valid:
                confidence *= 0.5

        # Extract referenced columns
        referenced = self._extract_referenced_columns(pyspark_code, request.source_schema)

        return GeneratedCode(
            pyspark_code=pyspark_code,
            sql_code=sql_code,
            intent=intent,
            confidence=confidence,
            explanation=explanation,
            warnings=warnings,
            referenced_columns=referenced,
        )

    def _validate_pyspark(self, code: str) -> Tuple[bool, List[str]]:
        """Validate PySpark code syntax and safety."""
        warnings = []

        # Check for syntax errors
        try:
            ast.parse(code)
        except SyntaxError as e:
            return False, [f"Syntax error: {e}"]

        # Check for dangerous operations
        dangerous_patterns = [
            (r'\bexec\b', "Contains 'exec' - potential security risk"),
            (r'\beval\b', "Contains 'eval' - potential security risk"),
            (r'\bos\.\b', "Contains 'os.' - potential security risk"),
            (r'\bsubprocess\b', "Contains 'subprocess' - potential security risk"),
            (r'\.write\b', "Contains write operation - verify destination"),
        ]

        for pattern, warning in dangerous_patterns:
            if re.search(pattern, code):
                warnings.append(warning)

        # Check for common PySpark issues
        if 'collect()' in code:
            warnings.append("Uses collect() - may cause memory issues on large datasets")
        if '.show(' in code:
            warnings.append("Uses show() - should be removed for production")

        return len([w for w in warnings if 'security risk' in w]) == 0, warnings

    def _extract_referenced_columns(self, code: str, schema: Dict[str, str]) -> List[str]:
        """Extract column names referenced in code."""
        referenced = []
        for column in schema.keys():
            # Check various ways columns might be referenced
            patterns = [
                f'"{column}"',
                f"'{column}'",
                f"F.col({column!r})",
                f"df[{column!r}]",
                f"df.{column}",
            ]
            for pattern in patterns:
                if pattern in code:
                    referenced.append(column)
                    break
        return list(set(referenced))

    def _generate_template_based(self, request: TransformRequest, intent: TransformIntent) -> GeneratedCode:
        """Generate code using templates when LLM is unavailable."""
        templates = {
            TransformIntent.AGGREGATE: """
# Aggregation transform
from pyspark.sql import functions as F

# Group by and aggregate
df = df.groupBy("group_column").agg(
    F.sum("value_column").alias("total"),
    F.count("*").alias("count"),
    F.avg("value_column").alias("average")
)
""",
            TransformIntent.WINDOW: """
# Window transform
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define window specification
window_spec = Window.partitionBy("partition_column").orderBy(F.col("order_column").desc())

# Apply window functions
df = df.withColumn("row_num", F.row_number().over(window_spec))
df = df.withColumn("running_total", F.sum("value_column").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
""",
            TransformIntent.JOIN: """
# Join transform
# Assuming df is the left table and lookup_df is the right table

df = df.join(
    lookup_df,
    on=df["join_key"] == lookup_df["join_key"],
    how="left"
).drop(lookup_df["join_key"])
""",
            TransformIntent.SCD: """
# SCD Type 2 transform
from pyspark.sql import functions as F
import hashlib

# Add hash for change detection
hash_cols = ["tracked_col1", "tracked_col2"]
df = df.withColumn("_hash", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in hash_cols]), 256))

# Add SCD columns
df = df.withColumn("effective_from", F.current_timestamp())
df = df.withColumn("effective_to", F.lit("9999-12-31").cast("timestamp"))
df = df.withColumn("is_current", F.lit(True))
""",
        }

        code = templates.get(intent, "# Complex transform - manual implementation required\ndf = df")

        return GeneratedCode(
            pyspark_code=code,
            intent=intent,
            confidence=0.5,
            explanation=f"Template-based code for {intent.value} transform. Review and customize.",
            warnings=["Template code - requires customization for your specific use case"],
        )

    def generate_aggregate_code(
        self,
        group_by: List[str],
        aggregations: List[Dict[str, str]],
        having: Optional[str] = None,
    ) -> str:
        """
        Generate aggregation PySpark code.

        Args:
            group_by: Columns to group by
            aggregations: List of {"column": "col", "function": "sum", "alias": "total"}
            having: Optional HAVING clause condition
        """
        agg_exprs = []
        for agg in aggregations:
            func = agg['function']
            col = agg['column']
            alias = agg.get('alias', f"{func}_{col}")

            if func == 'count':
                agg_exprs.append(f'F.count("{col}").alias("{alias}")')
            elif func == 'sum':
                agg_exprs.append(f'F.sum("{col}").alias("{alias}")')
            elif func == 'avg':
                agg_exprs.append(f'F.avg("{col}").alias("{alias}")')
            elif func == 'min':
                agg_exprs.append(f'F.min("{col}").alias("{alias}")')
            elif func == 'max':
                agg_exprs.append(f'F.max("{col}").alias("{alias}")')
            elif func == 'first':
                agg_exprs.append(f'F.first("{col}").alias("{alias}")')
            elif func == 'collect_list':
                agg_exprs.append(f'F.collect_list("{col}").alias("{alias}")')

        group_cols = ', '.join(f'"{c}"' for c in group_by)
        agg_str = ',\n    '.join(agg_exprs)

        code = f"""from pyspark.sql import functions as F

df = df.groupBy({group_cols}).agg(
    {agg_str}
)"""

        if having:
            code += f'\ndf = df.filter("{having}")'

        return code

    def generate_join_code(
        self,
        left_alias: str,
        right_table: str,
        join_keys: List[Dict[str, str]],
        join_type: str = "left",
        select_columns: Optional[List[str]] = None,
    ) -> str:
        """
        Generate join PySpark code.

        Args:
            left_alias: Alias for left DataFrame (df)
            right_table: Right table/path to load
            join_keys: List of {"left": "col1", "right": "col2"}
            join_type: left, right, inner, outer, cross
            select_columns: Columns to select after join
        """
        join_conditions = " & ".join(
            f'({left_alias}["{k["left"]}"] == right_df["{k["right"]}"])'
            for k in join_keys
        )

        code = f"""from pyspark.sql import functions as F

# Load right table
right_df = spark.read.json("{right_table}")

# Perform join
df = {left_alias}.join(
    right_df,
    on={join_conditions},
    how="{join_type}"
)"""

        if select_columns:
            cols = ', '.join(f'"{c}"' for c in select_columns)
            code += f'\n\n# Select columns\ndf = df.select({cols})'

        return code

    def generate_window_code(
        self,
        partition_by: List[str],
        order_by: List[Dict[str, str]],
        functions: List[Dict[str, Any]],
    ) -> str:
        """
        Generate window function PySpark code.

        Args:
            partition_by: Columns to partition by
            order_by: List of {"column": "col", "direction": "desc"}
            functions: List of {"type": "row_number/rank/lag/sum", "column": "col", "alias": "name"}
        """
        partition_cols = ', '.join(f'"{c}"' for c in partition_by)
        order_cols = ', '.join(
            f'F.col("{o["column"]}").{"desc" if o.get("direction") == "desc" else "asc"}()'
            for o in order_by
        )

        func_code = []
        for f in functions:
            ftype = f['type']
            alias = f['alias']
            col = f.get('column', '')

            if ftype == 'row_number':
                func_code.append(f'df = df.withColumn("{alias}", F.row_number().over(window_spec))')
            elif ftype == 'rank':
                func_code.append(f'df = df.withColumn("{alias}", F.rank().over(window_spec))')
            elif ftype == 'dense_rank':
                func_code.append(f'df = df.withColumn("{alias}", F.dense_rank().over(window_spec))')
            elif ftype == 'lag':
                offset = f.get('offset', 1)
                func_code.append(f'df = df.withColumn("{alias}", F.lag("{col}", {offset}).over(window_spec))')
            elif ftype == 'lead':
                offset = f.get('offset', 1)
                func_code.append(f'df = df.withColumn("{alias}", F.lead("{col}", {offset}).over(window_spec))')
            elif ftype == 'sum':
                func_code.append(f'df = df.withColumn("{alias}", F.sum("{col}").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)))')

        code = f"""from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define window specification
window_spec = Window.partitionBy({partition_cols}).orderBy({order_cols})

# Apply window functions
{chr(10).join(func_code)}"""

        return code
