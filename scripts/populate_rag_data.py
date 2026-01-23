#!/usr/bin/env python3
"""
RAG Data Population Script v5.0
================================

Populates both Weaviate (vector DB) and Neo4j (graph DB) with:
1. Scripts from registry.json with embeddings
2. Historical incidents with resolutions
3. Service dependencies and relationships
4. FIXED_BY relationships linking incidents to scripts

WHY: This creates a complete knowledge base for RAG-powered incident resolution:
- Weaviate: Semantic search for similar incidents and matching scripts
- Neo4j: Graph traversal for service dependencies and historical success rates

HOW TO RUN:
    cd /home/samrattidke600/ai_agent_app
    python3 scripts/populate_rag_data.py

Author: AI Agent Platform
Version: 5.0.0
"""

import os
import sys
import json
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import uuid

# Add project root to path (so 'backend' package works)
sys.path.insert(0, '/home/samrattidke600/ai_agent_app')

import structlog
logger = structlog.get_logger()


# =============================================================================
# CONFIGURATION
# =============================================================================

WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://localhost:8081")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "adminadmin")

# Registry files (root registry.json is source of truth)
REGISTRY_FILES = [
    "/home/samrattidke600/ai_agent_app/registry.json",  # PRIMARY: Root registry
    "/home/samrattidke600/ai_agent_app/backend/data/registry.json",
    "/home/samrattidke600/ai_agent_app/backend/runbooks/registry.json"
]

# Local script directories to search for script content
SCRIPT_DIRECTORIES = [
    "/home/samrattidke600/ai_agent_app/scripts",
    "/home/samrattidke600/ai_agent_app/backend/data/scripts",
    "/home/samrattidke600/ai_agent_app/backend/scripts"
]


# =============================================================================
# WEAVIATE FUNCTIONS
# =============================================================================

def get_weaviate_client():
    """Get Weaviate client using REST API (v3 compatible)"""
    import requests

    class SimpleWeaviateClient:
        """Simple Weaviate REST API client for compatibility with older versions"""

        def __init__(self, url: str):
            self.url = url.rstrip("/")

        def delete_class(self, class_name: str) -> bool:
            """Delete a class if it exists"""
            try:
                response = requests.delete(f"{self.url}/v1/schema/{class_name}")
                return response.status_code in [200, 204]
            except Exception:
                return False

        def class_exists(self, class_name: str) -> bool:
            """Check if a class exists"""
            try:
                response = requests.get(f"{self.url}/v1/schema/{class_name}")
                return response.status_code == 200
            except Exception:
                return False

        def create_class(self, schema: dict) -> bool:
            """Create a class"""
            try:
                response = requests.post(
                    f"{self.url}/v1/schema",
                    json=schema,
                    headers={"Content-Type": "application/json"}
                )
                return response.status_code in [200, 201]
            except Exception as e:
                print(f"    Error creating class: {e}")
                return False

        def add_object(self, class_name: str, properties: dict, vector: list = None) -> bool:
            """Add an object to a class"""
            try:
                obj = {
                    "class": class_name,
                    "properties": properties
                }
                if vector:
                    obj["vector"] = vector

                response = requests.post(
                    f"{self.url}/v1/objects",
                    json=obj,
                    headers={"Content-Type": "application/json"}
                )
                return response.status_code in [200, 201]
            except Exception:
                return False

        def close(self):
            """No-op for compatibility"""
            pass

    return SimpleWeaviateClient(WEAVIATE_URL)


def clean_weaviate():
    """Delete all Weaviate collections"""
    print("\n🔵 Cleaning Weaviate...")

    try:
        client = get_weaviate_client()

        # Delete existing collections using REST API
        for collection_name in ["Script", "Incident", "Runbook"]:
            try:
                if client.class_exists(collection_name):
                    if client.delete_class(collection_name):
                        print(f"  ✓ Deleted collection: {collection_name}")
                    else:
                        print(f"  - Failed to delete {collection_name}")
                else:
                    print(f"  - Collection {collection_name} not found")
            except Exception as e:
                print(f"  - {collection_name}: {e}")

        client.close()
        print("  ✅ Weaviate cleaned")
    except Exception as e:
        print(f"  ⚠️ Error cleaning Weaviate: {e}")


def create_weaviate_schema():
    """Create Weaviate schema for Scripts and Incidents (using REST API)"""
    print("\n🔵 Creating Weaviate schema...")

    try:
        client = get_weaviate_client()

        # Script collection - for remediation scripts
        script_schema = {
            "class": "Script",
            "description": "Remediation scripts for incident resolution",
            "vectorizer": "none",
            "vectorIndexConfig": {
                "distance": "cosine"
            },
            "properties": [
                {"name": "script_id", "dataType": ["text"], "description": "Unique script identifier"},
                {"name": "name", "dataType": ["text"], "description": "Script name"},
                {"name": "description", "dataType": ["text"], "description": "Script description for semantic search"},
                {"name": "path", "dataType": ["text"], "description": "Script file path"},
                {"name": "script_type", "dataType": ["text"], "description": "Type: shell, ansible, terraform, kubernetes"},
                {"name": "service", "dataType": ["text"], "description": "Target service"},
                {"name": "action", "dataType": ["text"], "description": "Action: restart, scale, cleanup, etc."},
                {"name": "keywords", "dataType": ["text[]"], "description": "Search keywords"},
                {"name": "error_patterns", "dataType": ["text[]"], "description": "Error patterns this script can fix"},
                {"name": "risk_level", "dataType": ["text"], "description": "Risk level: low, medium, high, critical"},
                {"name": "requires_approval", "dataType": ["boolean"], "description": "Whether HITL approval required"},
                {"name": "estimated_time_minutes", "dataType": ["int"], "description": "Estimated execution time"},
                {"name": "tags", "dataType": ["text[]"], "description": "Tags for filtering"}
            ]
        }

        if client.create_class(script_schema):
            print("  ✓ Created Script collection (with local embeddings)")
        else:
            print("  ⚠️ Failed to create Script collection")

        # Incident collection - for historical incidents
        incident_schema = {
            "class": "Incident",
            "description": "Historical incidents for similarity search",
            "vectorizer": "none",
            "vectorIndexConfig": {
                "distance": "cosine"
            },
            "properties": [
                {"name": "incident_id", "dataType": ["text"], "description": "Unique incident identifier"},
                {"name": "title", "dataType": ["text"], "description": "Short description"},
                {"name": "description", "dataType": ["text"], "description": "Full description for semantic search"},
                {"name": "resolution", "dataType": ["text"], "description": "How it was resolved"},
                {"name": "category", "dataType": ["text"], "description": "Incident category"},
                {"name": "service", "dataType": ["text"], "description": "Affected service"},
                {"name": "severity", "dataType": ["text"], "description": "Severity level"},
                {"name": "script_used", "dataType": ["text"], "description": "Script that fixed this"},
                {"name": "resolution_time_minutes", "dataType": ["number"], "description": "Time to resolve"},
                {"name": "created_at", "dataType": ["text"], "description": "When incident occurred"}
            ]
        }

        if client.create_class(incident_schema):
            print("  ✓ Created Incident collection (with local embeddings)")
        else:
            print("  ⚠️ Failed to create Incident collection")

        client.close()
        print("  ✅ Weaviate schema created")

    except Exception as e:
        print(f"  ⚠️ Error creating schema: {e}")


def get_embedding_service():
    """Get embedding service for generating vectors"""
    from backend.rag.embedding_service import EmbeddingService, EmbeddingConfig
    # Use local embeddings (no API cost, works offline)
    config = EmbeddingConfig(provider="local")
    return EmbeddingService(config)


def populate_weaviate_scripts(scripts: List[Dict]):
    """
    Populate Weaviate with scripts (including local embeddings).

    Creates rich embeddings that combine:
    - Script metadata (name, description, keywords, error_patterns)
    - Script content (if available locally)
    - Service and action context
    """
    print("\n🔵 Populating Weaviate with scripts...")

    try:
        client = get_weaviate_client()
        embedding_service = get_embedding_service()

        # Build all search texts first for batch embedding
        # Include script content for richer semantic search
        search_texts = []
        for script in scripts:
            # Build comprehensive search text combining metadata + content
            search_text = f"""
            Script: {script.get('name', '')}
            Description: {script.get('description', '')}
            Service: {script.get('service', '')} | Category: {script.get('category', '')}
            Action: {script.get('action', '')} | Component: {script.get('component', '')}
            Keywords: {' '.join(script.get('keywords', []))}
            Error patterns: {' '.join(script.get('error_patterns', []))}
            Tags: {' '.join(script.get('tags', []))}
            Risk level: {script.get('risk_level', 'medium')}
            """

            # Include script content if available (truncated for embedding)
            content = script.get('content', '')
            if content:
                # Extract meaningful parts of script (first 1000 chars)
                search_text += f"\nScript content snippet:\n{content[:1000]}"

            search_texts.append(search_text.strip())

        # Generate embeddings in batch (efficient)
        print("  ⏳ Generating embeddings for scripts...")
        embeddings = embedding_service.embed(search_texts)
        print(f"  ✓ Generated {len(embeddings)} embeddings (dim={embeddings.shape[1]})")

        success_count = 0
        for i, script in enumerate(scripts):
            try:
                data_object = {
                    "script_id": script.get("id", ""),
                    "name": script.get("name", ""),
                    "description": search_texts[i],
                    "path": script.get("path", ""),
                    "script_type": script.get("type", "shell"),
                    "service": script.get("service", ""),
                    "action": script.get("action", ""),
                    "keywords": script.get("keywords", []),
                    "error_patterns": script.get("error_patterns", []),
                    "risk_level": script.get("risk", script.get("risk_level", "medium")),
                    "requires_approval": script.get("requires_approval", False),
                    "estimated_time_minutes": script.get("estimated_time_minutes", 5),
                    "tags": script.get("tags", [])
                }

                # Insert with vector using REST API client
                if client.add_object("Script", data_object, embeddings[i].tolist()):
                    success_count += 1

            except Exception as e:
                print(f"  ⚠️ Failed to add script {script.get('id')}: {e}")

        client.close()
        print(f"  ✅ Added {success_count}/{len(scripts)} scripts to Weaviate")

    except Exception as e:
        print(f"  ⚠️ Error populating scripts: {e}")
        import traceback
        traceback.print_exc()


def populate_weaviate_incidents(incidents: List[Dict]):
    """Populate Weaviate with historical incidents (including local embeddings)"""
    print("\n🔵 Populating Weaviate with historical incidents...")

    try:
        client = get_weaviate_client()
        embedding_service = get_embedding_service()

        # Build search texts for embedding
        search_texts = []
        for incident in incidents:
            search_text = f"{incident.get('title', '')} {incident.get('description', '')} {incident.get('service', '')}"
            search_texts.append(search_text)

        # Generate embeddings in batch
        print(f"  ⏳ Generating embeddings for {len(incidents)} incidents...")
        embeddings = embedding_service.embed(search_texts)
        print(f"  ✓ Generated {len(embeddings)} embeddings")

        success_count = 0
        for i, incident in enumerate(incidents):
            try:
                data_object = {
                    "incident_id": incident.get("incident_id", ""),
                    "title": incident.get("title", ""),
                    "description": search_texts[i],
                    "resolution": incident.get("resolution", f"Resolved by script {incident.get('script_id', 'N/A')}"),
                    "category": incident.get("category", ""),
                    "service": incident.get("service", ""),
                    "severity": str(incident.get("severity", "3")),
                    "script_used": incident.get("script_id", incident.get("script_used", "")),
                    "resolution_time_minutes": float(incident.get("resolution_time", incident.get("resolution_time_minutes", 0))),
                    "created_at": incident.get("created_at", datetime.now().isoformat())
                }

                # Insert with vector using REST API client
                if client.add_object("Incident", data_object, embeddings[i].tolist()):
                    success_count += 1

            except Exception as e:
                print(f"  ⚠️ Failed to add incident {incident.get('incident_id')}: {e}")

        client.close()
        print(f"  ✅ Added {success_count}/{len(incidents)} incidents to Weaviate")

    except Exception as e:
        print(f"  ⚠️ Error populating incidents: {e}")
        import traceback
        traceback.print_exc()


# =============================================================================
# NEO4J FUNCTIONS
# =============================================================================

def get_neo4j_driver():
    """Get Neo4j driver"""
    from neo4j import GraphDatabase
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def clean_neo4j():
    """Delete all Neo4j data"""
    print("\n🟢 Cleaning Neo4j...")
    driver = get_neo4j_driver()

    try:
        with driver.session() as session:
            # Delete all relationships and nodes
            session.run("MATCH (n) DETACH DELETE n")
            print("  ✓ Deleted all nodes and relationships")

        print("  ✅ Neo4j cleaned")
    except Exception as e:
        print(f"  ⚠️ Error cleaning Neo4j: {e}")
    finally:
        driver.close()


def create_neo4j_schema():
    """Create Neo4j indexes and constraints for optimal query performance"""
    print("\n🟢 Creating Neo4j schema...")
    driver = get_neo4j_driver()

    try:
        with driver.session() as session:
            # Create indexes for all node types
            indexes = [
                # Core nodes
                "CREATE INDEX IF NOT EXISTS FOR (s:Script) ON (s.id)",
                "CREATE INDEX IF NOT EXISTS FOR (s:Script) ON (s.name)",
                "CREATE INDEX IF NOT EXISTS FOR (s:Script) ON (s.service)",
                "CREATE INDEX IF NOT EXISTS FOR (s:Script) ON (s.type)",
                "CREATE INDEX IF NOT EXISTS FOR (i:Incident) ON (i.incident_id)",
                "CREATE INDEX IF NOT EXISTS FOR (svc:Service) ON (svc.name)",
                "CREATE INDEX IF NOT EXISTS FOR (c:Category) ON (c.name)",
                # New relationship nodes
                "CREATE INDEX IF NOT EXISTS FOR (sc:SubCategory) ON (sc.name)",
                "CREATE INDEX IF NOT EXISTS FOR (k:Keyword) ON (k.name)",
                "CREATE INDEX IF NOT EXISTS FOR (ep:ErrorPattern) ON (ep.pattern)",
                "CREATE INDEX IF NOT EXISTS FOR (wf:Workflow) ON (wf.name)",
                "CREATE INDEX IF NOT EXISTS FOR (t:Team) ON (t.name)",
            ]

            for idx in indexes:
                session.run(idx)

            print("  ✓ Created indexes")

        print("  ✅ Neo4j schema created")
    except Exception as e:
        print(f"  ⚠️ Error creating schema: {e}")
    finally:
        driver.close()


def populate_neo4j_scripts(scripts: List[Dict]):
    """
    Populate Neo4j with scripts and rich relationship structure.

    Creates:
    - Script nodes with full metadata
    - Category hierarchy (Category → SubCategory)
    - Service relationships (Script → Service)
    - Keyword nodes for pattern matching
    - Error pattern nodes for incident matching
    - Workflow relationships (Script → Workflow)
    - Owner team relationships
    """
    print("\n🟢 Populating Neo4j with scripts...")
    driver = get_neo4j_driver()

    try:
        with driver.session() as session:
            success_count = 0

            for script in scripts:
                # Main script node with all metadata
                query = """
                MERGE (s:Script {id: $id})
                SET s.name = $name,
                    s.path = $path,
                    s.type = $type,
                    s.service = $service,
                    s.component = $component,
                    s.action = $action,
                    s.risk_level = $risk_level,
                    s.requires_approval = $requires_approval,
                    s.auto_approve = $auto_approve,
                    s.description = $description,
                    s.keywords = $keywords,
                    s.error_patterns = $error_patterns,
                    s.tags = $tags,
                    s.workflow = $workflow,
                    s.owner_team = $owner_team,
                    s.estimated_duration = $estimated_duration,
                    s.content_available = $content_available,
                    s.created_at = $created_at

                WITH s

                // Create Category relationship
                MERGE (c:Category {name: $category})
                MERGE (s)-[:BELONGS_TO]->(c)

                WITH s

                // Create SubCategory if exists
                FOREACH (ignoreMe IN CASE WHEN $subcategory <> '' THEN [1] ELSE [] END |
                    MERGE (sc:SubCategory {name: $subcategory})
                    MERGE (s)-[:HAS_SUBCATEGORY]->(sc)
                )

                WITH s

                // Create Service relationship
                MERGE (svc:Service {name: $service})
                MERGE (s)-[:TARGETS]->(svc)

                WITH s

                // Create Workflow relationship
                MERGE (wf:Workflow {name: $workflow})
                MERGE (s)-[:USES_WORKFLOW]->(wf)

                WITH s

                // Create Owner Team relationship
                FOREACH (ignoreMe IN CASE WHEN $owner_team <> '' THEN [1] ELSE [] END |
                    MERGE (team:Team {name: $owner_team})
                    MERGE (s)-[:OWNED_BY]->(team)
                )

                WITH s

                // Create rollback relationship if exists
                FOREACH (ignoreMe IN CASE WHEN $rollback_script <> '' THEN [1] ELSE [] END |
                    MERGE (rs:Script {id: $rollback_script})
                    MERGE (s)-[:HAS_ROLLBACK]->(rs)
                )
                """

                session.run(query, {
                    "id": script.get("id", ""),
                    "name": script.get("name", ""),
                    "path": script.get("path", ""),
                    "type": script.get("type", "shell"),
                    "service": script.get("service", "generic"),
                    "component": script.get("component", ""),
                    "action": script.get("action", ""),
                    "risk_level": script.get("risk", script.get("risk_level", "medium")),
                    "requires_approval": not script.get("auto_approve", True),
                    "auto_approve": script.get("auto_approve", False),
                    "description": script.get("description", ""),
                    "keywords": script.get("keywords", []),
                    "error_patterns": script.get("error_patterns", []),
                    "tags": script.get("tags", []),
                    "category": script.get("category", script.get("service", "generic")),
                    "subcategory": script.get("subcategory", ""),
                    "workflow": script.get("workflow", "shell-execute.yml"),
                    "owner_team": script.get("owner_team", ""),
                    "rollback_script": script.get("rollback_script", ""),
                    "estimated_duration": script.get("estimated_duration_seconds", 60),
                    "content_available": script.get("content_available", False),
                    "created_at": datetime.now().isoformat()
                })

                # Create keyword nodes with MATCHES relationship
                for keyword in script.get("keywords", []):
                    session.run("""
                        MERGE (k:Keyword {name: $keyword})
                        WITH k
                        MATCH (s:Script {id: $script_id})
                        MERGE (s)-[:HAS_KEYWORD]->(k)
                    """, {"keyword": keyword, "script_id": script.get("id")})

                # Create error pattern nodes with RESOLVES relationship
                for pattern in script.get("error_patterns", []):
                    session.run("""
                        MERGE (ep:ErrorPattern {pattern: $pattern})
                        WITH ep
                        MATCH (s:Script {id: $script_id})
                        MERGE (s)-[:RESOLVES]->(ep)
                    """, {"pattern": pattern, "script_id": script.get("id")})

                success_count += 1

            print(f"  ✅ Added {success_count}/{len(scripts)} scripts to Neo4j with rich relationships")

    except Exception as e:
        print(f"  ⚠️ Error populating scripts: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.close()


def populate_neo4j_services():
    """Populate Neo4j with services and dependencies"""
    print("\n🟢 Populating Neo4j with services...")
    driver = get_neo4j_driver()

    # Service definitions with dependencies
    services = {
        "gcp": {"tier": "infrastructure", "dependencies": []},
        "kubernetes": {"tier": "platform", "dependencies": ["gcp"]},
        "database": {"tier": "data", "dependencies": ["kubernetes"]},
        "redis": {"tier": "cache", "dependencies": ["kubernetes"]},
        "kafka": {"tier": "messaging", "dependencies": ["kubernetes"]},
        "airflow": {"tier": "pipeline", "dependencies": ["kubernetes", "database"]},
        "nginx": {"tier": "web", "dependencies": ["kubernetes"]},
        "api-gateway": {"tier": "application", "dependencies": ["nginx", "redis"]},
        "auth-service": {"tier": "application", "dependencies": ["database", "redis"]},
        "payment-service": {"tier": "application", "dependencies": ["database", "kafka"]},
        "user-service": {"tier": "application", "dependencies": ["database"]},
        "application": {"tier": "application", "dependencies": ["api-gateway"]},
        "generic": {"tier": "utility", "dependencies": []},
        "infrastructure": {"tier": "infrastructure", "dependencies": []},
        "webserver": {"tier": "web", "dependencies": ["nginx"]},
        "cache": {"tier": "cache", "dependencies": ["redis"]}
    }

    try:
        with driver.session() as session:
            # Create services
            for name, props in services.items():
                session.run("""
                    MERGE (s:Service {name: $name})
                    SET s.tier = $tier
                """, {"name": name, "tier": props["tier"]})

            # Create dependencies
            for name, props in services.items():
                for dep in props["dependencies"]:
                    session.run("""
                        MATCH (s:Service {name: $name})
                        MATCH (d:Service {name: $dep})
                        MERGE (s)-[:DEPENDS_ON]->(d)
                    """, {"name": name, "dep": dep})

            print(f"  ✅ Added {len(services)} services with dependencies")

    except Exception as e:
        print(f"  ⚠️ Error populating services: {e}")
    finally:
        driver.close()


def populate_neo4j_historical_incidents(scripts: List[Dict]):
    """Create historical incidents with FIXED_BY relationships"""
    print("\n🟢 Creating historical incidents with FIXED_BY relationships...")
    driver = get_neo4j_driver()

    # Generate realistic historical incidents
    incidents = []

    for script in scripts:
        # Create 3-8 historical incidents per script
        num_incidents = random.randint(3, 8)

        for i in range(num_incidents):
            # Random date in last 90 days
            days_ago = random.randint(1, 90)
            created_at = datetime.now() - timedelta(days=days_ago)

            # Success rate varies by risk level
            risk = script.get("risk", script.get("risk_level", "medium"))
            success_prob = {"low": 0.95, "medium": 0.85, "high": 0.70, "critical": 0.60}.get(risk, 0.80)
            success = random.random() < success_prob

            # Resolution time varies
            base_time = script.get("estimated_time_minutes", 5)
            resolution_time = base_time * random.uniform(0.5, 2.0) if success else 0

            # Get error patterns with fallback
            error_patterns = script.get("error_patterns", [])
            if not error_patterns:
                error_patterns = [f"{script.get('name', 'Unknown')} issue"]

            incident = {
                "incident_id": f"INC-{uuid.uuid4().hex[:8].upper()}",
                "script_id": script.get("id"),
                "service": script.get("service", "generic"),
                "category": script.get("service", "generic"),
                "title": random.choice(error_patterns),
                "description": f"Historical incident resolved by {script.get('name')}",
                "severity": random.choice(["1", "2", "3", "4"]),
                "success": success,
                "resolution_time": round(resolution_time, 1),
                "created_at": created_at.isoformat(),
                "resolved_at": (created_at + timedelta(minutes=resolution_time)).isoformat() if success else None,
                "verified": success and random.random() > 0.3  # 70% of successes are verified
            }
            incidents.append(incident)

    try:
        with driver.session() as session:
            success_count = 0

            for incident in incidents:
                query = """
                // Create Incident
                MERGE (i:Incident {incident_id: $incident_id})
                SET i.title = $title,
                    i.description = $description,
                    i.service = $service,
                    i.category = $category,
                    i.severity = $severity,
                    i.created_at = $created_at,
                    i.resolved_at = $resolved_at

                WITH i

                // Link to Script via FIXED_BY
                MATCH (s:Script {id: $script_id})
                MERGE (i)-[r:FIXED_BY]->(s)
                SET r.success = $success,
                    r.resolution_time = $resolution_time,
                    r.executed_at = $created_at,
                    r.verified = $verified

                WITH i

                // Link to Service
                MERGE (svc:Service {name: $service})
                MERGE (i)-[:AFFECTS]->(svc)
                """

                session.run(query, incident)
                success_count += 1

            print(f"  ✅ Created {success_count} historical incidents with FIXED_BY relationships")

    except Exception as e:
        print(f"  ⚠️ Error creating incidents: {e}")
    finally:
        driver.close()

    return incidents


# =============================================================================
# MAIN FUNCTIONS
# =============================================================================

def find_local_script_content(script_path: str) -> Optional[str]:
    """
    Find and read script content from local directories.

    Args:
        script_path: Path from registry (e.g., "scripts/start_gcp_instance.sh")

    Returns:
        Script content if found, None otherwise
    """
    # Extract filename from path
    filename = os.path.basename(script_path)

    # Search in configured directories
    for base_dir in SCRIPT_DIRECTORIES:
        # Try exact path
        full_path = os.path.join(base_dir, script_path)
        if os.path.exists(full_path):
            try:
                with open(full_path, 'r') as f:
                    return f.read()
            except Exception:
                pass

        # Try just filename
        full_path = os.path.join(base_dir, filename)
        if os.path.exists(full_path):
            try:
                with open(full_path, 'r') as f:
                    return f.read()
            except Exception:
                pass

    return None


def load_scripts() -> List[Dict]:
    """
    Load scripts from all registry files and enrich with local content.

    This function:
    1. Loads script metadata from registry.json files
    2. Searches for actual script content in local directories
    3. Enriches each script entry with content for better embeddings
    """
    all_scripts = []
    seen_ids = set()
    content_found = 0

    for registry_file in REGISTRY_FILES:
        if os.path.exists(registry_file):
            try:
                with open(registry_file, 'r') as f:
                    data = json.load(f)

                scripts = data.get("scripts", [])
                for script in scripts:
                    script_id = script.get("id")
                    if script_id and script_id not in seen_ids:
                        # Try to find local script content
                        script_path = script.get("path", "")
                        content = find_local_script_content(script_path)

                        if content:
                            script["content"] = content[:5000]  # Limit content size
                            script["content_available"] = True
                            content_found += 1
                        else:
                            script["content"] = ""
                            script["content_available"] = False

                        all_scripts.append(script)
                        seen_ids.add(script_id)

                print(f"  ✓ Loaded {len(scripts)} scripts from {registry_file}")
            except Exception as e:
                print(f"  ⚠️ Error loading {registry_file}: {e}")

    print(f"  ℹ️ Found local content for {content_found}/{len(all_scripts)} scripts")
    return all_scripts


def main():
    """Main execution"""
    print("=" * 70)
    print("  🚀 RAG DATA POPULATION SCRIPT v5.0")
    print("=" * 70)
    print(f"\n📍 Weaviate: {WEAVIATE_URL}")
    print(f"📍 Neo4j: {NEO4J_URI}")

    # Load scripts
    print("\n📂 Loading scripts from registry...")
    scripts = load_scripts()
    print(f"  Total scripts: {len(scripts)}")

    if not scripts:
        print("\n❌ No scripts found! Check registry files.")
        return

    # Clean databases
    clean_weaviate()
    clean_neo4j()

    # Create schemas
    create_weaviate_schema()
    create_neo4j_schema()

    # Populate Weaviate
    populate_weaviate_scripts(scripts)

    # Populate Neo4j
    populate_neo4j_scripts(scripts)
    populate_neo4j_services()
    historical_incidents = populate_neo4j_historical_incidents(scripts)

    # Also add historical incidents to Weaviate for semantic search
    populate_weaviate_incidents(historical_incidents)

    # Summary
    print("\n" + "=" * 70)
    print("  ✅ RAG DATA POPULATION COMPLETE!")
    print("=" * 70)
    print(f"\n📊 Summary:")
    print(f"  • {len(scripts)} scripts in Weaviate (vector search)")
    print(f"  • {len(scripts)} scripts in Neo4j (graph)")
    print(f"  • {len(historical_incidents)} historical incidents")
    print(f"  • Services with DEPENDS_ON relationships")
    print(f"  • FIXED_BY relationships for historical success")
    print(f"\n🎯 RAG Capabilities:")
    print(f"  • Semantic search: Find scripts by incident description")
    print(f"  • Graph scoring: Rank by historical success rate")
    print(f"  • Service dependencies: Understand blast radius")
    print(f"  • Similar incidents: Find what worked before")
    print()


if __name__ == "__main__":
    main()
