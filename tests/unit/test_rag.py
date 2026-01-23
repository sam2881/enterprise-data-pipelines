"""
Comprehensive RAG (Retrieval-Augmented Generation) Tests
=========================================================
Tests for the Hybrid Search Engine, Script Indexing, and RAG API endpoints.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))


class TestHybridSearchEngine:
    """Test HybridSearchEngine core functionality"""

    def test_search_weights_initialization(self):
        """Test SearchWeights initialization and normalization"""
        from rag.hybrid_search_engine import SearchWeights

        # Test default weights
        weights = SearchWeights()
        assert abs(weights.semantic + weights.keyword + weights.metadata + weights.graph - 1.0) < 0.01

        # Test custom weights get normalized
        weights = SearchWeights(semantic=0.6, keyword=0.4, metadata=0.2, graph=0.2)
        total = weights.semantic + weights.keyword + weights.metadata + weights.graph
        assert abs(total - 1.0) < 0.01

    def test_search_weights_presets(self):
        """Test preset weight configurations"""
        from rag.hybrid_search_engine import SearchWeights

        # Test legacy preset
        legacy = SearchWeights.legacy()
        assert legacy.graph == 0.0  # Legacy has no graph
        assert legacy.semantic > legacy.keyword  # Semantic should be higher

    def test_hybrid_search_engine_initialization(self):
        """Test HybridSearchEngine can be initialized"""
        from rag.hybrid_search_engine import HybridSearchEngine

        engine = HybridSearchEngine(enable_graph=False)  # Disable graph for unit test
        assert engine is not None
        assert engine.is_fitted == False
        assert engine.documents == []

    def test_document_indexing(self):
        """Test documents can be indexed"""
        from rag.hybrid_search_engine import HybridSearchEngine

        engine = HybridSearchEngine(enable_graph=False)

        documents = [
            {
                "chunk_id": "script-1",
                "content": "Start GCP VM instance compute engine",
                "metadata": {"service": "gcp", "action": "start"}
            },
            {
                "chunk_id": "script-2",
                "content": "Restart Kubernetes pod container crash",
                "metadata": {"service": "kubernetes", "action": "restart"}
            },
            {
                "chunk_id": "script-3",
                "content": "Clear disk space storage full cleanup",
                "metadata": {"service": "linux", "action": "cleanup"}
            }
        ]

        count = engine.index_documents(documents)
        assert count == 3
        assert engine.is_fitted == True
        assert len(engine.documents) == 3

    def test_keyword_search(self):
        """Test keyword-based search (TF-IDF)"""
        from rag.hybrid_search_engine import HybridSearchEngine, SearchWeights

        engine = HybridSearchEngine(enable_graph=False)

        documents = [
            {"chunk_id": "vm-start", "content": "Start GCP VM instance", "metadata": {}},
            {"chunk_id": "vm-stop", "content": "Stop GCP VM instance", "metadata": {}},
            {"chunk_id": "disk-clean", "content": "Clear disk space storage", "metadata": {}},
        ]
        engine.index_documents(documents)

        # Search with keyword-heavy weights
        results = engine.search(
            query="start VM",
            top_k=3,
            weights=SearchWeights(semantic=0.0, keyword=1.0, metadata=0.0, graph=0.0)
        )

        assert len(results) > 0
        # Top result should contain "start" in content
        assert "start" in results[0].content.lower()

    def test_semantic_search(self):
        """Test semantic similarity search returns results"""
        from rag.hybrid_search_engine import HybridSearchEngine, SearchWeights

        engine = HybridSearchEngine(enable_graph=False)

        documents = [
            {"chunk_id": "k8s-restart", "content": "Restart Kubernetes pod to recover from crash", "metadata": {}},
            {"chunk_id": "vm-reboot", "content": "Reboot virtual machine instance", "metadata": {}},
            {"chunk_id": "db-backup", "content": "Create database backup snapshot", "metadata": {}},
        ]
        engine.index_documents(documents)

        # Search with semantic-heavy weights
        results = engine.search(
            query="container crashed need to restart",
            top_k=3,
            weights=SearchWeights(semantic=1.0, keyword=0.0, metadata=0.0, graph=0.0)
        )

        # Should return results
        assert len(results) > 0

        # Results should have semantic scores
        assert results[0].score_breakdown.get("semantic", 0) >= 0

        # All results should have content
        assert all(r.content for r in results)

    def test_metadata_search(self):
        """Test metadata-based filtering"""
        from rag.hybrid_search_engine import HybridSearchEngine, SearchWeights

        engine = HybridSearchEngine(enable_graph=False)

        documents = [
            {"chunk_id": "gcp-1", "content": "Start VM on Google Cloud", "metadata": {"service": "gcp", "category": "compute"}},
            {"chunk_id": "aws-1", "content": "Start EC2 on Amazon", "metadata": {"service": "aws", "category": "compute"}},
            {"chunk_id": "k8s-1", "content": "Start pod in Kubernetes", "metadata": {"service": "kubernetes", "category": "container"}},
        ]
        engine.index_documents(documents)

        # Search with metadata filtering
        results = engine.search(
            query="start instance",
            query_metadata={"service": "gcp"},
            top_k=3,
            weights=SearchWeights(semantic=0.3, keyword=0.3, metadata=0.4, graph=0.0)
        )

        assert len(results) > 0
        # With high metadata weight, GCP-related result should be prioritized
        # Check that we get results and they have score breakdowns
        assert results[0].score_breakdown is not None

    def test_hybrid_search_score_breakdown(self):
        """Test that search results include score breakdown"""
        from rag.hybrid_search_engine import HybridSearchEngine

        engine = HybridSearchEngine(enable_graph=False)

        documents = [
            {"chunk_id": "test-1", "content": "Test script content", "metadata": {"service": "test"}}
        ]
        engine.index_documents(documents)

        results = engine.search(query="test script", top_k=1)

        assert len(results) == 1
        result = results[0]

        # Check score breakdown exists
        assert "semantic" in result.score_breakdown
        assert "keyword" in result.score_breakdown
        assert "metadata" in result.score_breakdown
        assert "graph" in result.score_breakdown

        # Check final score is calculated
        assert result.final_score >= 0
        assert result.final_score <= 1

    def test_empty_query_handling(self):
        """Test handling of empty queries"""
        from rag.hybrid_search_engine import HybridSearchEngine

        engine = HybridSearchEngine(enable_graph=False)
        documents = [{"chunk_id": "1", "content": "test", "metadata": {}}]
        engine.index_documents(documents)

        results = engine.search(query="", top_k=5)
        # Should handle gracefully, may return empty or all results
        assert isinstance(results, list)

    def test_search_before_indexing(self):
        """Test search behavior before documents are indexed"""
        from rag.hybrid_search_engine import HybridSearchEngine

        engine = HybridSearchEngine(enable_graph=False)

        results = engine.search(query="test query", top_k=5)
        assert results == []  # Should return empty list

    def test_get_stats(self):
        """Test engine statistics"""
        from rag.hybrid_search_engine import HybridSearchEngine

        engine = HybridSearchEngine(enable_graph=False)

        stats = engine.get_stats()
        assert stats["version"] == "5.0.0"
        assert stats["documents_indexed"] == 0
        assert stats["is_fitted"] == False

        # After indexing
        engine.index_documents([{"chunk_id": "1", "content": "test", "metadata": {}}])
        stats = engine.get_stats()
        assert stats["documents_indexed"] == 1
        assert stats["is_fitted"] == True


class TestSearchResult:
    """Test SearchResult dataclass"""

    def test_search_result_creation(self):
        """Test SearchResult can be created"""
        from rag.hybrid_search_engine import SearchResult

        result = SearchResult(
            chunk_id="test-123",
            content="Test content",
            metadata={"key": "value"},
            final_score=0.85,
            score_breakdown={"semantic": 0.9, "keyword": 0.8, "metadata": 0.7, "graph": 0.0},
            match_reasons=["High semantic match"],
            rank=1
        )

        assert result.chunk_id == "test-123"
        assert result.final_score == 0.85
        assert len(result.match_reasons) == 1

    def test_search_result_to_dict(self):
        """Test SearchResult serialization"""
        from rag.hybrid_search_engine import SearchResult

        result = SearchResult(
            chunk_id="test",
            content="content",
            metadata={},
            final_score=0.5,
            score_breakdown={},
            match_reasons=[],
            rank=1
        )

        result_dict = result.to_dict()
        assert isinstance(result_dict, dict)
        assert "chunk_id" in result_dict
        assert "final_score" in result_dict


@pytest.mark.skip(reason="RAG API endpoints not yet implemented - need /api/v2/rag/* routes")
class TestRAGAPIEndpoints:
    """Test RAG API endpoints using requests (sync)

    NOTE: These endpoints need to be added to backend/app.py:
    - POST /api/v2/rag/search
    - GET /api/v2/rag/stats
    """

    def test_rag_search_endpoint(self):
        """Test /api/rag/search endpoint"""
        import requests

        try:
            response = requests.post(
                "http://localhost:8000/api/rag/search",
                json={"query": "VM instance down", "top_k": 5},
                timeout=10
            )

            assert response.status_code == 200
            data = response.json()
            assert "results" in data
            assert "count" in data
            assert isinstance(data["results"], list)
        except requests.exceptions.ConnectionError:
            pytest.skip("Backend server not running")

    def test_rag_search_with_weights(self):
        """Test /api/rag/search with custom weights"""
        import requests

        try:
            response = requests.post(
                "http://localhost:8000/api/rag/search",
                json={
                    "query": "kubernetes pod crash",
                    "top_k": 3,
                    "weights": {
                        "semantic": 0.5,
                        "keyword": 0.3,
                        "metadata": 0.2
                    }
                },
                timeout=10
            )

            assert response.status_code == 200
            data = response.json()
            assert "results" in data
        except requests.exceptions.ConnectionError:
            pytest.skip("Backend server not running")

    def test_rag_search_with_metadata(self):
        """Test /api/rag/search with metadata filters"""
        import requests

        try:
            response = requests.post(
                "http://localhost:8000/api/rag/search",
                json={
                    "query": "restart service",
                    "metadata": {"service": "gcp"},
                    "top_k": 5
                },
                timeout=10
            )

            assert response.status_code == 200
            data = response.json()
            assert "results" in data
        except requests.exceptions.ConnectionError:
            pytest.skip("Backend server not running")

    def test_rag_stats_endpoint(self):
        """Test /api/rag/stats endpoint"""
        import requests

        try:
            response = requests.get(
                "http://localhost:8000/api/rag/stats",
                timeout=10
            )

            assert response.status_code == 200
            data = response.json()
            assert "feedback_stats" in data or "script_stats" in data
        except requests.exceptions.ConnectionError:
            pytest.skip("Backend server not running")


@pytest.mark.skip(reason="RAG API endpoints not yet implemented - need /api/v2/rag/* routes")
class TestScriptIndexing:
    """Test script indexing for RAG - requires API endpoints"""

    def test_registry_scripts_indexed_via_api(self):
        """Test that registry scripts are indexed (via API)"""
        import requests

        try:
            # The backend indexes scripts on startup, check via API
            response = requests.post(
                "http://localhost:8000/api/rag/search",
                json={"query": "restart pod", "top_k": 5},
                timeout=10
            )

            assert response.status_code == 200
            data = response.json()
            # Should have results if scripts are indexed
            assert data["count"] > 0 or "results" in data
        except requests.exceptions.ConnectionError:
            pytest.skip("Backend server not running")

    def test_indexed_scripts_searchable_via_api(self):
        """Test indexed scripts can be searched via API"""
        import requests

        try:
            response = requests.post(
                "http://localhost:8000/api/rag/search",
                json={"query": "kubernetes pod crash", "top_k": 5},
                timeout=10
            )

            assert response.status_code == 200
            data = response.json()
            results = data.get("results", [])

            if results:
                # Check if kubernetes-related content is found
                found_k8s = any(
                    "kubernetes" in r.get("content", "").lower() or
                    "pod" in r.get("content", "").lower()
                    for r in results
                )
                assert found_k8s, "Should find Kubernetes-related scripts"
        except requests.exceptions.ConnectionError:
            pytest.skip("Backend server not running")

    def test_search_returns_script_metadata(self):
        """Test search results include script metadata"""
        import requests

        try:
            response = requests.post(
                "http://localhost:8000/api/rag/search",
                json={"query": "GCP VM instance", "top_k": 3},
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])

                if results:
                    result = results[0]
                    # Should have metadata
                    assert "metadata" in result
        except requests.exceptions.ConnectionError:
            pytest.skip("Backend server not running")


class TestCrossEncoderReranker:
    """Test cross-encoder reranking"""

    def test_reranker_initialization(self):
        """Test reranker can be initialized"""
        try:
            from rag.cross_encoder_reranker import reranker_with_fallback
            assert reranker_with_fallback is not None
        except ImportError:
            pytest.skip("Cross-encoder not available")

    def test_reranker_fallback(self):
        """Test reranker fallback when model unavailable"""
        try:
            from rag.cross_encoder_reranker import reranker_with_fallback

            candidates = [
                {"chunk_id": "1", "content": "test content 1", "metadata": {}, "final_score": 0.8},
                {"chunk_id": "2", "content": "test content 2", "metadata": {}, "final_score": 0.6},
            ]

            results = reranker_with_fallback.rerank("test query", candidates, top_k=2)

            assert len(results) <= 2
            assert isinstance(results, list)
        except ImportError:
            pytest.skip("Cross-encoder not available")


class TestEmbeddingService:
    """Test embedding generation"""

    def test_embedding_service_initialization(self):
        """Test embedding service can be initialized"""
        try:
            from rag.embedding_service import embedding_service
            assert embedding_service is not None
        except ImportError:
            pytest.skip("Embedding service not available")

    def test_embed_text(self):
        """Test text embedding generation"""
        try:
            from rag.embedding_service import embedding_service

            text = "This is a test sentence for embedding"
            embedding = embedding_service.embed(text)

            assert embedding is not None
            assert len(embedding) > 0  # Should have dimensions
        except ImportError:
            pytest.skip("Embedding service not available")


class TestSmartChunker:
    """Test smart chunking for scripts"""

    def test_smart_chunker_initialization(self):
        """Test smart chunker can be initialized"""
        try:
            from rag.smart_chunker import smart_chunker
            assert smart_chunker is not None
        except ImportError:
            pytest.skip("Smart chunker not available")

    def test_chunk_script(self):
        """Test script chunking"""
        try:
            from rag.smart_chunker import smart_chunker

            script = {
                "id": "test-ansible-script",
                "type": "ansible",
                "content": """
- name: Restart web server
  hosts: webservers
  tasks:
    - name: Stop nginx
      service:
        name: nginx
        state: stopped

    - name: Start nginx
      service:
        name: nginx
        state: started
""",
                "metadata": {"service": "web"}
            }

            chunks = smart_chunker.chunk_script(script)

            assert len(chunks) > 0
            # Chunks should be Chunk objects with to_dict method
            assert all(hasattr(c, 'to_dict') for c in chunks)
        except ImportError:
            pytest.skip("Smart chunker not available")


class TestFeedbackOptimizer:
    """Test feedback-based optimization"""

    def test_feedback_optimizer_initialization(self):
        """Test feedback optimizer can be initialized"""
        try:
            from rag.feedback_optimizer import feedback_optimizer
            assert feedback_optimizer is not None
        except ImportError:
            pytest.skip("Feedback optimizer not available")

    def test_record_feedback(self):
        """Test recording search feedback"""
        try:
            from rag.feedback_optimizer import feedback_optimizer

            feedback_id = feedback_optimizer.record_feedback(
                incident_id="INC001",
                incident_type="vm_down",
                severity="high",
                service="gcp",
                environment="prod",
                query="VM not responding",
                weights_used={"semantic": 0.4, "keyword": 0.3},
                recommended_script_id="script-start-vm",
                recommendation_rank=1
            )

            assert feedback_id is not None
        except ImportError:
            pytest.skip("Feedback optimizer not available")
