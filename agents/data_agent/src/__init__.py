"""
Enterprise Agentic Data Engineering Platform - Data Agent.

This is the main entry point for the Data Agent package. It provides
a deterministic pipeline generation system that:
- Accepts validated JSON intent from UI
- Plans pipeline execution strategy
- Generates Airflow DAGs and Spark jobs from templates
- Validates generated artifacts
- Deploys with human approval for production

Key Modules:
    - agents: LangGraph agent implementations
    - graphs: LangGraph workflow definitions
    - state: Pydantic state models
    - metadata: PostgreSQL repository layer
    - generators: Jinja2-based code generators
    - validators: Artifact validation logic
    - deployers: Git and CI/CD deployment
    - config: Environment configuration
    - utils: Logging and exception utilities
"""

__version__ = "1.0.0"
__author__ = "Enterprise Data Platform Team"

# Re-export commonly used items at package level
from src.config import get_settings, Settings
from src.utils import get_logger, DataAgentError
from src.agents import BaseAgent

__all__ = [
    "__version__",
    "__author__",
    "get_settings",
    "Settings",
    "get_logger",
    "DataAgentError",
    "BaseAgent",
]
