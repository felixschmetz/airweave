"""Test step implementations with improved deletion testing."""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any, Dict

from monke.core.test_config import TestConfig
from monke.utils.logging import get_logger


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
        
        # Optional post-create delay to allow upstream APIs to propagate data
        delay_seconds = 0
        try:
            delay_override = (
                self.config.connector.config_fields.get("post_create_sleep_seconds")
                if hasattr(self.config, "connector") and hasattr(self.config.connector, "config_fields")
                else None
            )
            if delay_override is not None:
                delay_seconds = int(delay_override)
        except Exception:
            delay_seconds = 0

        if delay_seconds > 0:
            self.logger.info(f"⏸️ Waiting {delay_seconds}s after creation to allow source API propagation")
            await asyncio.sleep(delay_seconds)
        
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
            # Get the unique token that was embedded in the entity
            expected_token = entity.get("token")
            if not expected_token:
                self.logger.warning("⚠️ No token found in entity, falling back to filename")
                expected_token = (entity.get("path") or "").split("/")[-1]
            
            self.logger.info(f"🔍 Looking for token: {expected_token}")
            
            # Search for the token - but we might need to get more results since token might be embedded
            try:
                # First try direct token search
                search_resp = client.collections.search_collection(
                    self.config._collection_readable_id,
                    query=expected_token,
                    score_threshold=0.0,
                    limit=1000,  # Get more results since we're doing substring matching
                )
                search_results = search_resp.model_dump()
                
                results = search_results.get("results", [])
                self.logger.info(f"📊 Token search returned {len(results)} results")
                
                # Check if token is in any of the results by serializing payload
                for result in results:
                    payload = result.get("payload", {})
                    # Serialize the entire payload to string for substring matching
                    payload_str = str(payload).lower()
                    
                    if expected_token.lower() in payload_str:
                        name = payload.get("name") or payload.get("title") or "Unknown"
                        self.logger.info(f"✅ Found token '{expected_token}' in: {name}")
                        
                        # Show which field contains the token
                        for key, value in payload.items():
                            if value and expected_token.lower() in str(value).lower():
                                self.logger.info(f"   - Token found in field '{key}': {str(value)[:100]}...")
                                break
                        return True
                
                # If direct search didn't find it, try a broader search
                if len(results) == 0:
                    self.logger.info("🔍 Direct token search returned no results, trying broader search...")
                    # Try searching for just part of the token or with wildcards
                    search_resp = client.collections.search_collection(
                        self.config._collection_readable_id,
                        query="",  # Empty query to get some results
                        score_threshold=0.0,
                        limit=1000,
                    )
                    search_results = search_resp.model_dump()
                    results = search_results.get("results", [])
                    
                    self.logger.info(f"📊 Broad search returned {len(results)} results")
                    
                    # Check these results too
                    for result in results:
                        payload = result.get("payload", {})
                        payload_str = str(payload).lower()
                        
                        if expected_token.lower() in payload_str:
                            name = payload.get("name") or payload.get("title") or "Unknown"
                            self.logger.info(f"✅ Found token '{expected_token}' via broad search in: {name}")
                            return True
                
                # Token not found in any search results
                self.logger.warning(f"⚠️ Token '{expected_token}' not found in any of the {len(results)} search results")
                return False
                    
            except Exception as search_error:
                self.logger.error(f"❌ Error during token search: {str(search_error)}")
                # Try one more time with semantic search as fallback
                self.logger.info("🔄 Falling back to semantic search...")
                
                search_results_resp = client.collections.search_collection(
                    self.config._collection_readable_id,
                    query=expected_token,
                    score_threshold=0.0,
                )
                search_results = search_results_resp.model_dump()
                
                if search_results.get("results"):
                    # Do substring matching on semantic search results
                    for result in search_results["results"]:
                        payload_str = str(result.get("payload", {})).lower()
                        if expected_token.lower() in payload_str:
                            self.logger.info(f"✅ Found token '{expected_token}' via semantic search fallback")
                            return True
                
                return False
                
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
            # Get the unique token that identifies this entity
            expected_token = entity.get("token")
            if not expected_token:
                self.logger.warning("⚠️ No token found in entity for deletion verification")
                # Fall back to ID or name
                expected_token = str(entity.get("id") or entity.get("gid") or entity.get("name", ""))
            
            if not expected_token:
                self.logger.error("❌ Cannot verify deletion - no identifying information")
                return False
            
            self.logger.info(f"🔍 Verifying deletion of entity with token: {expected_token}")
            
            # Search for the token directly
            try:
                search_resp = client.collections.search_collection(
                    self.config._collection_readable_id,
                    query=expected_token,
                    score_threshold=0.0,
                    limit=100,
                )
                search_results = search_resp.model_dump()
                
                results = search_results.get("results", [])
                self.logger.info(f"📊 Token search returned {len(results)} results")
                
                # Check if token exists in any of the results
                for result in results:
                    payload = result.get("payload", {})
                    payload_str = str(payload).lower()
                    
                    if expected_token.lower() in payload_str:
                        # Found the entity - it wasn't deleted!
                        name = payload.get("name") or payload.get("title") or "Unknown"
                        self.logger.warning(f"❌ Entity with token '{expected_token}' still exists: {name}")
                        
                        # Show where the token was found
                        for key, value in payload.items():
                            if value and expected_token.lower() in str(value).lower():
                                self.logger.warning(f"   - Token found in field '{key}': {str(value)[:100]}...")
                                break
                        return False
                
                # Token not found - entity was successfully deleted
                self.logger.info(f"✅ Entity with token '{expected_token}' confirmed deleted from Qdrant")
                return True
                    
            except Exception as search_error:
                self.logger.error(f"❌ Error during deletion verification search: {str(search_error)}")
                return False
            
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
            # Get the unique token that identifies this entity
            expected_token = entity.get("token")
            if not expected_token:
                # Fall back to filename for file-based entities
                expected_token = entity.get('path', '').split('/')[-1] if entity.get('path') else str(entity.get('id', ''))
            
            if not expected_token:
                self.logger.error("❌ Cannot verify entity presence - no identifying information")
                return False
            
            self.logger.info(f"🔍 Verifying entity still exists with token: {expected_token}")
            
            # Search for the token directly
            try:
                search_resp = client.collections.search_collection(
                    self.config._collection_readable_id,
                    query=expected_token,
                    score_threshold=0.0,
                    limit=100,
                )
                search_results = search_resp.model_dump()
                
                results = search_results.get("results", [])
                self.logger.info(f"📊 Token search returned {len(results)} results")
                
                # Check if token exists in any of the results
                for result in results:
                    payload = result.get("payload", {})
                    payload_str = str(payload).lower()
                    
                    if expected_token.lower() in payload_str:
                        # Found the entity
                        name = payload.get("name") or payload.get("title") or "Unknown"
                        self.logger.info(f"✅ Entity with token '{expected_token}' still exists: {name}")
                        return True
                
                # Token not found
                self.logger.warning(f"⚠️ Entity with token '{expected_token}' NOT found in search results")
                return False
                    
            except Exception as search_error:
                self.logger.error(f"❌ Error during entity presence verification: {str(search_error)}")
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
            # Get the unique token that identifies this entity
            expected_token = entity.get("token")
            if not expected_token:
                # Fall back to filename for GitHub entities
                expected_token = entity.get('path', '').split('/')[-1] if entity.get('path') else str(entity.get('id', ''))
            
            if not expected_token:
                self.logger.error("❌ Cannot verify deletion - no identifying information")
                return False
            
            self.logger.info(f"🔍 Verifying deletion of entity with identifier: {expected_token}")
            
            # Search for the token directly
            try:
                search_resp = client.collections.search_collection(
                    self.config._collection_readable_id,
                    query=expected_token,
                    score_threshold=0.0,
                    limit=100,
                )
                search_results = search_resp.model_dump()
                
                results = search_results.get("results", [])
                self.logger.info(f"📊 Token search returned {len(results)} results")
                
                # Check if token exists in any of the results
                for result in results:
                    payload = result.get("payload", {})
                    payload_str = str(payload).lower()
                    
                    if expected_token.lower() in payload_str:
                        # Found the entity - it wasn't deleted!
                        name = payload.get("name") or payload.get("title") or "Unknown"
                        self.logger.warning(f"❌ Entity with identifier '{expected_token}' still exists: {name}")
                        return False
                
                # Token not found - entity was successfully deleted
                self.logger.info(f"✅ Entity with identifier '{expected_token}' confirmed deleted")
                return True
                    
            except Exception as search_error:
                self.logger.error(f"❌ Error during deletion verification: {str(search_error)}")
                return False
            
        except Exception as e:
            self.logger.error(f"❌ Error verifying entity deletion: {str(e)}")
            return False
    
    async def _verify_collection_empty_of_test_data(self, client) -> bool:
        """Verify the Qdrant collection is empty of test data."""
        try:
            # Search for any test data patterns
            test_patterns = ["monke-test", "Monke Test"]
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
