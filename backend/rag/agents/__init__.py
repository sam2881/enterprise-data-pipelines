"""
RAG Swarm Agents

WHY: Implements distributed search using specialized agents:
- VectorAgent: Semantic similarity search (weight: 0.40)
- KeywordAgent: TF-IDF keyword matching (weight: 0.25)
- GraphAgent: Neo4j FIXED_BY relationships (weight: 0.25)
- MetadataAgent: Exact field matching (weight: 0.10)

HOW: Each agent runs as an A2A participant, receiving queries
and returning votes with results and confidence scores.
"""
from .vector_agent import VectorAgent
from .keyword_agent import KeywordAgent
from .graph_agent import GraphAgent
from .metadata_agent import MetadataAgent

__all__ = [
    "VectorAgent",
    "KeywordAgent",
    "GraphAgent",
    "MetadataAgent",
]
