"""Test flow execution engine with improved deletion testing."""

import time
from typing import Any, Dict, List

from datamonkey.core.test_config import TestConfig
from datamonkey.core.test_steps import TestStepFactory
from datamonkey.utils.logging import get_logger

# FAT FUCKING TODO: MOVE ALL CONNECTORS TO COMPOSIO AUTH 
class TestFlow:
    """Executes a test flow based on configuration."""
    
    def __init__(self, config: TestConfig):
        """Initialize the test flow."""
        self.config = config
        self.logger = get_logger(f"test_flow.{config.name}")
        self.step_factory = TestStepFactory()
        self.metrics = {}
        self.warnings = []
        
    @classmethod
    def create(cls, config: TestConfig) -> "TestFlow":
        """Create a test flow from configuration."""
        # For now, return a generic test flow
        # Later, we can implement connector-specific flows if needed
        return cls(config)
    
    async def execute(self):
        """Execute the test flow."""
        self.logger.info(f"🚀 Executing test flow: {self.config.name}")
        self.logger.info(f"🔄 Test flow steps: {self.config.test_flow.steps}")
        
        try:
            # Execute each step in sequence
            for step_name in self.config.test_flow.steps:
                try:
                    await self._execute_step(step_name)
                except Exception as e:
                    self.logger.error(f"❌ Step {step_name} failed: {e}")
                    raise
            
            self.logger.info(f"✅ Test flow completed: {self.config.name}")
        except Exception as e:
            self.logger.error(f"❌ Test flow execution failed: {e}")
            # Ensure cleanup happens even on failure
            try:
                await self.cleanup()
            except Exception as cleanup_error:
                self.logger.error(f"❌ Cleanup failed after test failure: {cleanup_error}")
            raise
    
    async def _execute_step(self, step_name: str):
        """Execute a single test step."""
        self.logger.info(f"🔄 Executing step: {step_name}")
        
        step = self.step_factory.create_step(step_name, self.config)
        start_time = time.time()
        
        try:
            await step.execute()
            duration = time.time() - start_time
            
            self.metrics[f"{step_name}_duration"] = duration
            self.logger.info(f"✅ Step {step_name} completed in {duration:.2f}s")
            
        except Exception:
            duration = time.time() - start_time
            self.metrics[f"{step_name}_duration"] = duration
            self.metrics[f"{step_name}_failed"] = True
            raise
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get test execution metrics."""
        return self.metrics.copy()
    
    def get_warnings(self) -> List[str]:
        """Get test execution warnings."""
        return self.warnings.copy()
    
    async def setup(self) -> bool:
        """Set up the test environment."""
        self.logger.info("🔧 Setting up test environment")
        
        # Create the connector instance using the registry
        from datamonkey.bongos.connector_registry import ConnectorRegistry
        from datamonkey.core.credentials_resolver import resolve_credentials
        
        try:
            # Create bongo instance
            resolved_creds = await resolve_credentials(
                self.config.connector.type, self.config.connector.auth_fields
            )
            bongo = ConnectorRegistry.create(
                self.config.connector.type,
                resolved_creds,
                entity_count=self.config.entity_count,
                **self.config.connector.config_fields
            )
            
            # Store bongo in config for steps to access
            self.config._bongo = bongo
            
            # Create Airweave client
            from datamonkey.utils.airweave_client import AirweaveClient
            airweave_client = AirweaveClient()
            self.config._airweave_client = airweave_client
            
            # Set up collection and source connection
            await self._setup_infrastructure(bongo, airweave_client)
            
            self.logger.info("✅ Test environment setup completed")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup test environment: {e}")
            return False
    
    async def _setup_infrastructure(self, bongo, airweave_client):
        """Set up Airweave infrastructure."""
        # Create collection
        collection_name = f"datamonkey-{self.config.connector.type}-test-{int(time.time())}"
        collection = await airweave_client.create_collection({"name": collection_name})
        self.config._collection_id = collection["id"]
        self.config._collection_readable_id = collection["readable_id"]
        
        # Create source connection (provider-agnostic)
        import os
        
        # Check if auth_fields are explicitly provided in config
        has_explicit_auth = bool(self.config.connector.auth_fields)
        use_provider = os.getenv("DM_AUTH_PROVIDER") is not None and not has_explicit_auth

        if has_explicit_auth:
            self.logger.info(f"🔑 Using explicit auth fields from config for {self.config.connector.type}")
        elif use_provider:
            self.logger.info(f"🔐 Using auth provider: {os.getenv('DM_AUTH_PROVIDER')}")
        else:
            self.logger.info("⚠️  No auth configured - will attempt with empty auth_fields")

        if use_provider:
            # 1) Create or reuse auth provider connection
            auth_provider_short_name = os.getenv("DM_AUTH_PROVIDER")
            auth_provider_api_key = os.getenv("DM_AUTH_PROVIDER_API_KEY")
            if not auth_provider_api_key:
                raise ValueError("DM_AUTH_PROVIDER_API_KEY must be set when DM_AUTH_PROVIDER is configured")

            auth_provider_resp = await airweave_client.connect_auth_provider({
                "name": f"{auth_provider_short_name.title()} {int(time.time())}",
                "short_name": auth_provider_short_name,
                "auth_fields": {"api_key": auth_provider_api_key},
            })
            readable_auth_provider_id = auth_provider_resp["readable_id"]

            # 2) Create source connection using the auth provider
            auth_provider_config = {}
            auth_config_id = os.getenv("DM_AUTH_PROVIDER_AUTH_CONFIG_ID")
            account_id = os.getenv("DM_AUTH_PROVIDER_ACCOUNT_ID")
            
            if auth_config_id:
                auth_provider_config["auth_config_id"] = auth_config_id
            if account_id:
                auth_provider_config["account_id"] = account_id

            payload = {
                "name": f"{self.config.connector.type.title()} Test Connection {int(time.time())}",
                "short_name": self.config.connector.type,
                "collection": self.config._collection_readable_id,
                "config_fields": self.config.connector.config_fields,
                "auth_provider": readable_auth_provider_id,
                "auth_provider_config": auth_provider_config,
            }
        else:
            payload = {
                "name": f"{self.config.connector.type.title()} Test Connection {int(time.time())}",
                "short_name": self.config.connector.type,
                "collection": self.config._collection_readable_id,
                "auth_fields": self.config.connector.auth_fields,
                "config_fields": self.config.connector.config_fields,
            }

        source_connection = await airweave_client.create_source_connection(payload)
        self.config._source_connection_id = source_connection["id"]
    
    async def cleanup(self) -> bool:
        """Clean up the test environment."""
        try:
            self.logger.info("🧹 Cleaning up test environment")
            
            if hasattr(self.config, '_source_connection_id'):
                # Delete source connection
                await self.config._airweave_client.delete_source_connection(
                    self.config._source_connection_id
                )
                self.logger.info("✅ Deleted source connection")
            
            if hasattr(self.config, '_collection_readable_id'):
                # Delete collection
                await self.config._airweave_client.delete_collection(
                    self.config._collection_readable_id
                )
                self.logger.info("✅ Deleted test collection")
            
            self.logger.info("✅ Test environment cleanup completed")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to cleanup test environment: {e}")
            return False
