"""
Business Logic Parsers Package.

Converts diverse business logic inputs to executable PySpark code:
- SQL queries from analysts
- DTSX/SSIS packages from migrations
- Natural language from business users
- Column mapping spreadsheets
"""

from .business_logic_parser import (
    # Data classes
    SourceTable,
    JoinCondition,
    Aggregation,
    ColumnTransform,
    FilterCondition,
    ParsedLogic,
    # Parsers
    BusinessLogicParser,
    SQLParser,
    DTSXParser,
    NaturalLanguageParser,
    ColumnMappingParser,
    # Factory
    BusinessLogicParserFactory,
)

__all__ = [
    # Data classes
    "SourceTable",
    "JoinCondition",
    "Aggregation",
    "ColumnTransform",
    "FilterCondition",
    "ParsedLogic",
    # Parsers
    "BusinessLogicParser",
    "SQLParser",
    "DTSXParser",
    "NaturalLanguageParser",
    "ColumnMappingParser",
    # Factory
    "BusinessLogicParserFactory",
]
