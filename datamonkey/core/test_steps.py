"""Test step implementations with improved deletion testing."""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any, Dict

from datamonkey.core.test_config import TestConfig
from datamonkey.utils.logging import get_logger


class TestStep(ABC):
    """Abstract base class for test steps."""
    
    def __init__(self, config: TestConfig):
        """Initialize the test step."""
        self.config = config
        self.logger = get_logger(f"test_step.{self.__class__.__name__}")

    def _display_name(self, entity: Dict[str, Any]) -> str:
        """Return a human-readable identifier for an entity regardless of type."""
        return (
            entity.get("path")
            or entity.get("title")
            or entity.get("id")
            or entity.get("url")
            or "<unknown>"
        )
    
    @abstractmethod
    async def execute(self):
        """Execute the test step."""
        pass


class CreateStep(TestStep):
    """Create test entities step."""
    
    async def execute(self):
        """Create test entities via the connector."""
        self.logger.info("🥁 Creating test entities")
        
        # Get the appropriate bongo for this connector
        bongo = self._get_bongo()
        
        # Create entities
        entities = await bongo.create_entities()
        
        self.logger.info(f"✅ Created {len(entities)} test entities")
        
        # Store entities for later steps and on bongo for deletes
        self.config._created_entities = entities
        if hasattr(self.config, '_bongo'):
            self.config._bongo.created_entities = entities
    
    def _get_bongo(self):
        """Get the bongo instance for this connector."""
        return getattr(self.config, '_bongo', None)


class SyncStep(TestStep):
    """Sync data to Airweave step."""
    
    async def execute(self):
        """Trigger sync and wait for completion."""
        self.logger.info("🔄 Syncing data to Airweave")
        
        # Get Airweave client
        client = self._get_airweave_client()
        
        # Trigger sync via SDK
        client.source_connections.run_source_connection(self.config._source_connection_id)
        
        # Wait for completion
        await self._wait_for_sync_completion(client)
        
        self.logger.info("✅ Sync completed")
    
    def _get_airweave_client(self):
        """Get the Airweave client instance."""
        return getattr(self.config, '_airweave_client', None)
    
    async def _wait_for_sync_completion(self, client, timeout_seconds: int = 300):
        """Wait for sync to complete."""
        self.logger.info("⏳ Waiting for sync to complete...")
        
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            try:
                # Check sync job status via SDK
                jobs = client.source_connections.list_source_connection_jobs(self.config._source_connection_id)
                
                if jobs:
                    latest_job = jobs[0]
                    # Pydantic typed model; use attribute access
                    status = getattr(latest_job, "status", None)
                    
                    self.logger.info(f"🔍 Found job with status: {status}")
                    
                    if status == "completed":
                        self.logger.info("✅ Sync completed successfully")
                        return
                    elif status == "failed":
                        error = getattr(latest_job, "error", "Unknown error")
                        raise Exception(f"Sync failed: {error}")
                    elif status in ["created", "pending", "in_progress"]:
                        self.logger.info(f"⏳ Sync status: {status}")
                        await asyncio.sleep(5)
                        continue
                else:
                    self.logger.info("⏳ No jobs found yet, waiting...")
                
                await asyncio.sleep(5)
                
            except Exception as e:
                self.logger.warning(f"⚠️ Error checking sync status: {str(e)}")
                await asyncio.sleep(5)
        
        raise Exception("Sync timeout reached")


class VerifyStep(TestStep):
    """Verify data in Qdrant step."""
    
    async def execute(self):
        """Verify entities exist in Qdrant."""
        self.logger.info("🔍 Verifying entities in Qdrant")
        
        # Get Airweave client
        client = self._get_airweave_client()
        
        # Verify each entity by embedded verification token (file path only as a last-resort fallback)
        for entity in self.config._created_entities:
            is_present = await self._verify_entity_in_qdrant(client, entity)
            if not is_present:
                raise Exception(f"Entity {self._display_name(entity)} not found in Qdrant")
            self.logger.info(f"✅ Entity {self._display_name(entity)} verified in Qdrant")
        
        self.logger.info("✅ All entities verified in Qdrant")
    

    
    def _get_airweave_client(self):
        """Get the Airweave client instance."""
        return getattr(self.config, '_airweave_client', None)
    
    async def _verify_entity_in_qdrant(self, client, entity: Dict[str, Any]) -> bool:
        """Verify a specific entity exists in Qdrant by searching for its token/content."""
        try:
            # Prefer unique token if present; then expected_content; then filename as last resort
            expected_token = entity.get("token")
            expected_content = entity.get("expected_content", "")
            filename = (entity.get("path") or "").split("/")[-1]
            query_string = expected_token or expected_content or filename
            
            self.logger.info(f"🔍 Searching for entity by: {query_string}")
            if expected_token:
                self.logger.info(f"🔍 Expected token: {expected_token}")
            else:
                self.logger.info(f"🔍 Expected filename (fallback): {filename}")
            
            # First, let's see what's actually in the collection
            self.logger.info("🔍 Checking what's actually in the collection...")
            all_results_resp = client.collections.search_collection(
                self.config._collection_readable_id,
                query="datamonkey",
                score_threshold=0.1,
            )
            all_results = all_results_resp.model_dump()
            
            if all_results.get("results"):
                self.logger.info(f"🔍 Collection contains {len(all_results['results'])} total documents")
                for i, result in enumerate(all_results["results"][:5]):
                    payload = result.get("payload", {})
                    name = payload.get("name") or payload.get("title") or "NO_NAME"
                    path = payload.get("path", "NO_PATH")
                    score = result.get("score", 0)
                    self.logger.info(f"   {i+1}. Name: {name}, Path: {path}, Score: {score:.3f}")
            else:
                self.logger.warning("⚠️ Collection appears to be empty")
            
            # Search for the entity using its expected content (semantic search)
            # Try with reasonable score threshold first
            # Use configurable threshold if available
            initial_threshold = self.config.verification_config.get('score_threshold', 0.5)
            search_results_resp = client.collections.search_collection(
                self.config._collection_readable_id,
                query=query_string,
                score_threshold=initial_threshold,
            )
            search_results = search_results_resp.model_dump()
            
            self.logger.info("📊 Search results (threshold %.1f): %s" % (initial_threshold, len(search_results.get('results', []))))
            
            # If no results, try with lower threshold
            if not search_results.get("results"):
                self.logger.info("🔍 Trying with lower score threshold...")
                search_results = client.collections.search_collection(
                    self.config._collection_readable_id,
                    query=query_string,
                    score_threshold=0.1,
                ).model_dump()
                self.logger.info("📊 Search results (threshold 0.1): %s" % len(search_results.get('results', [])))
            
            # If still no results, try empty search to see what's in the collection
            if not search_results.get("results"):
                self.logger.info("🔍 Trying empty search to see collection contents...")
                empty_search = client.collections.search_collection(
                    self.config._collection_readable_id,
                    query="",
                    score_threshold=0.0,
                ).model_dump()
                self.logger.info(f"📊 Empty search results: {len(empty_search.get('results', []))}")
                
                if empty_search.get("results"):
                    self.logger.info("🔍 Collection contents from empty search:")
                    for i, result in enumerate(empty_search["results"][:5]):
                        payload = result.get("payload", {})
                        name = payload.get("name", "NO_NAME")
                        path = payload.get("path", "NO_PATH")
                        score = result.get("score", 0)
                        self.logger.info(f"   {i+1}. Name: {name}, Path: {path}, Score: {score:.3f}")
            
            if not search_results.get("results"):
                self.logger.warning(f"⚠️ No search results for content: {expected_content}")
                return False
            
            # Check if we have reasonable-scoring results (use lower threshold)
            reasonable_score_results = [
                result for result in search_results["results"] 
                if result.get("score", 0) >= 0.1  # Lower threshold like old architecture
            ]
            
            if not reasonable_score_results:
                best_score = max([r.get('score', 0) for r in search_results['results']]) if search_results['results'] else 0
                self.logger.warning(f"⚠️ No reasonable-scoring results for content: {expected_content} (best score: {best_score})")
                return False
            
            # Show what we found
            self.logger.info(f"🔍 Found {len(reasonable_score_results)} results for content:")
            for i, result in enumerate(reasonable_score_results):
                payload = result.get("payload", {})
                name = payload.get("name") or payload.get("title") or "NO_NAME"
                path = payload.get("path", "NO_PATH")
                score = result.get("score", 0)
                self.logger.info(f"   {i+1}. Name: {name}, Path: {path}, Score: {score:.3f}")
            
            # Verify that our expected token/content exists in the reasonable-scoring results
            expected_token = (expected_token or expected_content or "")
            expected_file_found = False
            
            # Check if the expected token appears in common payload text fields
            for result in reasonable_score_results:
                payload = result.get("payload", {})
                # Coerce to text safely (handles None, lists, dicts)
                def _to_text(value) -> str:
                    if value is None:
                        return ""
                    if isinstance(value, str):
                        return value
                    if isinstance(value, list):
                        return "\n".join(_to_text(v) for v in value)
                    if isinstance(value, dict):
                        return str(value)
                    return str(value)

                result_name = _to_text(payload.get("name") or payload.get("title"))
                # Consider various possible content fields across connectors
                candidate_fields = [
                    payload.get("content"),
                    payload.get("notes"),
                    payload.get("body"),
                    payload.get("text"),
                    payload.get("description"),
                    payload.get("comment"),
                    payload.get("title"),
                    payload.get("md_content"),
                    payload.get("md_title"),
                ]
                result_text_fields = [_to_text(v) for v in candidate_fields]
                
                # Check if the expected token appears in the name or any content field
                if expected_token and (
                    expected_token in result_name or any(expected_token in t for t in result_text_fields)
                ):
                    expected_file_found = True
                    result_score = result.get("score", 0)
                    self.logger.info(f"✅ Found entity with expected token '{expected_token}' in results with score {result_score:.3f}: {result_name}")
                    break
            
            if not expected_file_found:
                self.logger.warning(f"⚠️ Expected token/content '{expected_token or filename}' not found in any reasonable-scoring results")
                return False
            
            self.logger.info(f"✅ Found {len(reasonable_score_results)} reasonable-scoring results for content")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Verification failed for {self._display_name(entity)}: {str(e)}")
            return False


class UpdateStep(TestStep):
    """Update test entities step."""
    
    async def execute(self):
        """Update test entities via the connector."""
        self.logger.info("📝 Updating test entities")
        
        # Get the appropriate bongo
        bongo = self._get_bongo()
        
        # Update entities
        updated_entities = await bongo.update_entities()
        
        self.logger.info(f"✅ Updated {len(updated_entities)} test entities")
        
        # Store updated entities
        self.config._updated_entities = updated_entities
    
    def _get_bongo(self):
        """Get the bongo instance for this connector."""
        return getattr(self.config, '_bongo', None)


class PartialDeleteStep(TestStep):
    """Partial deletion step - delete subset of entities based on test size."""
    
    async def execute(self):
        """Delete a subset of entities based on test size configuration."""
        self.logger.info("🗑️ Executing partial deletion")
        
        # Get the appropriate bongo
        bongo = self._get_bongo()
        
        # Determine deletion count based on test size
        deletion_count = self._calculate_partial_deletion_count()
        
        # Select entities to delete (first N entities)
        entities_to_delete = self.config._created_entities[:deletion_count]
        entities_to_keep = self.config._created_entities[deletion_count:]
        
        self.logger.info(f"🗑️ Deleting {len(entities_to_delete)} entities: {[self._display_name(e) for e in entities_to_delete]}")
        self.logger.info(f"💾 Keeping {len(entities_to_keep)} entities: {[self._display_name(e) for e in entities_to_keep]}")
        
        # Delete selected entities
        deleted_paths = await bongo.delete_specific_entities(entities_to_delete)
        
        # Store for verification steps
        self.config._partially_deleted_entities = entities_to_delete
        self.config._remaining_entities = entities_to_keep
        
        self.logger.info(f"✅ Partial deletion completed: {len(deleted_paths)} entities deleted")
    
    def _get_bongo(self):
        """Get the bongo instance for this connector."""
        return getattr(self.config, '_bongo', None)
    
    def _calculate_partial_deletion_count(self) -> int:
        """Calculate how many entities to delete based on configuration."""
        # Use the new simplified deletion configuration
        return self.config.deletion.partial_delete_count


class VerifyPartialDeletionStep(TestStep):
    """Verify that partially deleted entities are removed from Qdrant."""
    
    async def execute(self):
        """Verify deleted entities are gone and remaining entities are still present."""
        self.logger.info("🔍 Verifying partial deletion")
        
        if not self.config.deletion.verify_partial_deletion:
            self.logger.info("⏭️ Skipping partial deletion verification (disabled in config)")
            return
        
        # Get Airweave client
        client = self._get_airweave_client()
        
        # No delay needed - Qdrant is updated instantly after sync completes
        
        # Log what we expect to find deleted
        self.logger.info("🔍 Expecting these entities to be deleted:")
        for entity in self.config._partially_deleted_entities:
            self.logger.info(f"   - {self._display_name(entity)} (token: {entity.get('token', 'N/A')})")
        
        # Verify deleted entities are removed
        for entity in self.config._partially_deleted_entities:
            is_removed = await self._verify_entity_deleted_from_qdrant(client, entity)
            if not is_removed:
                raise Exception(f"Entity {self._display_name(entity)} still exists in Qdrant after deletion")
            self.logger.info(f"✅ Entity {self._display_name(entity)} confirmed removed from Qdrant")
        
        self.logger.info("✅ Partial deletion verification completed")
    
    def _get_airweave_client(self):
        """Get the Airweave client instance."""
        return getattr(self.config, '_airweave_client', None)
    
    async def _verify_entity_deleted_from_qdrant(self, client, entity: Dict[str, Any]) -> bool:
        """Verify a specific entity has been removed from Qdrant."""
        try:
            # Use token if available, otherwise use title/url/id
            search_query = entity.get('token', entity.get('expected_content', ''))
            if not search_query:
                # Fallbacks in order of reliability
                fallback = (
                    entity.get('title')
                    or entity.get('url')
                    or entity.get('id')
                    or entity.get('path')
                    or ''
                )
                # If it's a path-like string, reduce to filename
                if isinstance(fallback, str) and '/' in fallback:
                    fallback = fallback.rsplit('/', 1)[-1]
                search_query = fallback
            
            search_results = client.collections.search_collection(
                self.config._collection_readable_id,
                query=search_query,
                score_threshold=0.1,
            ).model_dump()
            
            # Check if any results contain this entity
            results = search_results.get("results", [])
            for result in results:
                payload = result.get("payload", {})
                result_name = payload.get("name", "")
                result_content = payload.get("content", "")
                
                # For token-based entities, check if token appears in name or content
                if entity.get('token') and (entity['token'] in result_name or entity['token'] in result_content):
                    self.logger.warning(f"⚠️ Found entity with token {entity['token']}: {result_name}")
                    return False
                
                # For file-based entities, check if this result contains the same file number
                if isinstance(search_query, str) and search_query.startswith("datamonkey-test-"):
                    parts = search_query.split("-")
                    if len(parts) > 2:
                        file_number = parts[2]
                        if result_name.startswith(f"datamonkey-test-{file_number}-"):
                            self.logger.warning(f"⚠️ Found entity with same file number: {result_name}")
                            return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error verifying entity deletion: {str(e)}")
            return False


class VerifyRemainingEntitiesStep(TestStep):
    """Verify that remaining entities are still present in Qdrant."""
    
    async def execute(self):
        """Verify that entities not meant to be deleted are still present."""
        self.logger.info("🔍 Verifying remaining entities are still present")
        
        if not self.config.deletion.verify_remaining_entities:
            self.logger.info("⏭️ Skipping remaining entities verification (disabled in config)")
            return
        
        # Get Airweave client
        client = self._get_airweave_client()
        
        # Verify remaining entities are still present
        for entity in self.config._remaining_entities:
            is_present = await self._verify_entity_still_in_qdrant(client, entity)
            if not is_present:
                raise Exception(f"Entity {self._display_name(entity)} was incorrectly removed from Qdrant")
            self.logger.info(f"✅ Entity {self._display_name(entity)} confirmed still present in Qdrant")
        
        self.logger.info("✅ Remaining entities verification completed")
    
    def _get_airweave_client(self):
        """Get the Airweave client instance."""
        return getattr(self.config, '_airweave_client', None)
    
    async def _verify_entity_still_in_qdrant(self, client, entity: Dict[str, Any]) -> bool:
        """Verify a specific entity is still present in Qdrant."""
        try:
            # Use token if available, otherwise use filename
            search_query = entity.get('token', entity.get('expected_content', ''))
            if not search_query:
                # Fallback to filename for file-based entities
                search_query = entity['path'].split('/')[-1]
            
            search_results = client.collections.search_collection(
                self.config._collection_readable_id,
                query=search_query,
                score_threshold=0.2,
            ).model_dump()
            
            # Check if any results contain this entity
            results = search_results.get("results", [])
            for result in results:
                payload = result.get("payload", {})
                result_name = payload.get("name", "")
                result_content = payload.get("content", "")
                
                # For token-based entities, check if token appears in name or content
                if entity.get('token') and (entity['token'] in result_name or entity['token'] in result_content):
                    return True
                # For file-based entities, check exact filename match
                elif payload.get("name") == search_query:
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Error verifying entity presence: {str(e)}")
            return False


class CompleteDeleteStep(TestStep):
    """Complete deletion step - delete all remaining entities."""
    
    async def execute(self):
        """Delete all remaining test entities."""
        self.logger.info("🗑️ Executing complete deletion")
        
        # Get the appropriate bongo
        bongo = self._get_bongo()
        
        # Delete remaining entities
        remaining_entities = self.config._remaining_entities
        if not remaining_entities:
            self.logger.info("ℹ️ No remaining entities to delete")
            return
        
        self.logger.info(f"🗑️ Deleting remaining {len(remaining_entities)} entities")
        
        deleted_paths = await bongo.delete_specific_entities(remaining_entities)
        
        self.logger.info(f"✅ Complete deletion completed: {len(deleted_paths)} entities deleted")
    
    def _get_bongo(self):
        """Get the bongo instance for this connector."""
        return getattr(self.config, '_bongo', None)


class VerifyCompleteDeletionStep(TestStep):
    """Verify that all test entities are completely removed from Qdrant."""
    
    async def execute(self):
        """Verify Qdrant collection is empty of test data."""
        self.logger.info("🔍 Verifying complete deletion")
        
        if not self.config.deletion.verify_complete_deletion:
            self.logger.info("⏭️ Skipping complete deletion verification (disabled in config)")
            return
        
        # Get Airweave client
        client = self._get_airweave_client()
        
        # Verify all test entities are removed
        all_test_entities = (self.config._partially_deleted_entities + 
                           self.config._remaining_entities)
        
        for entity in all_test_entities:
            is_removed = await self._verify_entity_deleted_from_qdrant(client, entity)
            if not is_removed:
                raise Exception(f"Entity {self._display_name(entity)} still exists in Qdrant after complete deletion")
            self.logger.info(f"✅ Entity {self._display_name(entity)} confirmed removed from Qdrant")
        
        # Verify collection is essentially empty (only metadata entities might remain)
        collection_empty = await self._verify_collection_empty_of_test_data(client)
        if not collection_empty:
            self.logger.warning("⚠️ Qdrant collection still contains some data (may be metadata entities)")
        else:
            self.logger.info("✅ Qdrant collection confirmed empty of test data")
        
        self.logger.info("✅ Complete deletion verification completed")
    
    def _get_airweave_client(self):
        """Get the Airweave client instance."""
        return getattr(self.config, '_airweave_client', None)
    
    async def _verify_entity_deleted_from_qdrant(self, client, entity: Dict[str, Any]) -> bool:
        """Verify a specific entity has been removed from Qdrant."""
        try:
            # Search for the entity using its filename
            filename = entity['path'].split('/')[-1]
            search_results = client.collections.search_collection(
                self.config._collection_readable_id,
                query=filename,
                score_threshold=0.1,
            ).model_dump()
            
            # Check if any results contain this entity
            results = search_results.get("results", [])
            for result in results:
                payload = result.get("payload", {})
                if payload.get("name") == filename:
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error verifying entity deletion: {str(e)}")
            return False
    
    async def _verify_collection_empty_of_test_data(self, client) -> bool:
        """Verify the Qdrant collection is empty of test data."""
        try:
            # Search for any test data patterns
            test_patterns = ["datamonkey-test", "Datamonkey Test"]
            total_test_results = 0
            
            for pattern in test_patterns:
                search_results = client.collections.search_collection(
                    self.config._collection_readable_id,
                    query=pattern,
                    score_threshold=0.3,
                ).model_dump()
                
                results = search_results.get("results", [])
                total_test_results += len(results)
                
                if results:
                    self.logger.info(f"🔍 Found {len(results)} results for pattern '{pattern}'")
                    for result in results[:3]:  # Log first 3 results
                        payload = result.get("payload", {})
                        self.logger.info(f"   - {payload.get('name', 'Unknown')} (score: {result.get('score')})")
            
            if total_test_results == 0:
                self.logger.info("✅ No test data found in collection")
                return True
            else:
                self.logger.warning(f"⚠️ Found {total_test_results} test data results in collection")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error verifying collection emptiness: {str(e)}")
            return False


class TestStepFactory:
    """Factory for creating test steps."""
    
    _steps = {
        "create": CreateStep,
        "sync": SyncStep,
        "verify": VerifyStep,
        "update": UpdateStep,
        "partial_delete": PartialDeleteStep,
        "verify_partial_deletion": VerifyPartialDeletionStep,
        "verify_remaining_entities": VerifyRemainingEntitiesStep,
        "complete_delete": CompleteDeleteStep,
        "verify_complete_deletion": VerifyCompleteDeletionStep,
    }
    
    def create_step(self, step_name: str, config: TestConfig) -> TestStep:
        """Create a test step by name."""
        if step_name not in self._steps:
            raise ValueError(f"Unknown test step: {step_name}")
        
        step_class = self._steps[step_name]
        return step_class(config)
