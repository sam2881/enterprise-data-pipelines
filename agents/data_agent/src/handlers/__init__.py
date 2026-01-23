"""
Source Handlers Package.

Provides unified interface for reading data from multiple sources:
- CSV (with and without metadata files)
- EBCDIC/COBOL Copybook (mainframe data)
- Fixed-width text files
- JSON/JSONL
- Database tables (via JDBC)
- REST APIs
- Streaming (Kafka, Pub/Sub)
"""

from .source_handlers import (
    # Base classes
    BaseSourceHandler,
    SourceConfig,
    ParsedSchema,
    ParsedColumn,
    # Configuration classes
    CSVConfig,
    EBCDICConfig,
    FixedWidthConfig,
    JSONConfig,
    DatabaseConfig,
    APIConfig,
    StreamingConfig,
    # Handlers
    CSVSourceHandler,
    EBCDICSourceHandler,
    FixedWidthSourceHandler,
    JSONSourceHandler,
    DatabaseSourceHandler,
    APISourceHandler,
    StreamingSourceHandler,
    # Factory
    SourceHandlerFactory,
)

__all__ = [
    # Base
    "BaseSourceHandler",
    "SourceConfig",
    "ParsedSchema",
    "ParsedColumn",
    # Configs
    "CSVConfig",
    "EBCDICConfig",
    "FixedWidthConfig",
    "JSONConfig",
    "DatabaseConfig",
    "APIConfig",
    "StreamingConfig",
    # Handlers
    "CSVSourceHandler",
    "EBCDICSourceHandler",
    "FixedWidthSourceHandler",
    "JSONSourceHandler",
    "DatabaseSourceHandler",
    "APISourceHandler",
    "StreamingSourceHandler",
    # Factory
    "SourceHandlerFactory",
]
