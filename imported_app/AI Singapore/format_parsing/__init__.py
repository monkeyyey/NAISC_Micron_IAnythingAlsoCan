# log_pipeline/__init__.py
"""
log_pipeline package
Public API exports for log ingestion, processing, and staging.
"""

# Core services
from .ingestion import LogIngestionService
from .staging import StagingArea

# Optional utilities (if you want to expose them)
from .normalizer import *
from .detector import *
from .base_processor import *

# Expose processors at top-level (optional, for convenience)
from .processors import (
    LogTemplateTrie,
    KeyValueProcessor,
    LogfmtProcessor,
    DelimiterProcessor,
    CSVProcessor,
    TSVProcessor,
    JSONProcessor,
    XMLProcessor,
    YAMLProcessor,
    BinaryProcessor,
    SyslogProcessor,
)

# Define public API
__all__ = [
    "LogIngestionService",
    "StagingArea",
    "LogTemplateTrie",
    "KeyValueProcessor",
    "LogfmtProcessor",
    "DelimiterProcessor",
    "CSVProcessor",
    "TSVProcessor",
    "JSONProcessor",
    "XMLProcessor",
    "YAMLProcessor",
    "BinaryProcessor",
    "SyslogProcessor",
]
