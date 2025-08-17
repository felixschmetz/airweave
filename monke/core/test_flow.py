"""Test flow execution engine with structured events."""

import time
from typing import Any, Dict, List, Optional

from monke.core.test_config import TestConfig
from monke.core.test_steps import TestStepFactory
from monke.utils.logging import get_logger
from monke.core import events

class TestFlow:
    """Executes a test flow based on configuration."""
    
    def __init__(self, config: TestConfig, run_id: Optional[str] = None):
        """Initialize the test flow."""
        self.config = config
        self.logger = get_logger(f"test_flow.{config.name}")
        self.step_factory = TestStepFactory()
        self.metrics = {}
        self.warnings = []
        self.run_id = run_id or f"run-{int(time.time()*1000)}"
        
    @classmethod
    def create(cls, config: TestConfig, run_id: Optional[str] = None) -> "TestFlow":
        """Create a test flow from configuration."""
        # For now, return a generic test flow
        # Later, we can implement connector-specific flows if needed
        return cls(config, run_id=run_id)
    
    async def execute(self):
        """Execute the test flow."""
        self.logger.info(f"🚀 Executing test flow: {self.config.name}")
        self.logger.info(f"🔄 Test flow steps: {self.config.test_flow.steps}")
        await self._emit_event(
            "flow_started",
            extra={
                "name": self.config.name,
                "connector": self.config.connector.type,
                "steps": self.config.test_flow.steps,
                "entity_count": self.config.entity_count,
            },
        )
        
        try:
            # Execute each step in sequence
            for step_name in self.config.test_flow.steps:
                try:
                    await self._execute_step(step_name)
                except Exception as e:
                    self.logger.error(f"❌ Step {step_name} failed: {e}")
                    await self._emit_event(
                        "flow_failed",
                        extra={"error": str(e)}
                    )
                    raise
            
            self.logger.info(f"✅ Test flow completed: {self.config.name}")
            await self._emit_event("flow_completed")
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
        await self._emit_event("step_started", extra={"step": step_name})
        
        step = self.step_factory.create_step(step_name, self.config)
        start_time = time.time()
        
        try:
            await step.execute()
            duration = time.time() - start_time
            
            self.metrics[f"{step_name}_duration"] = duration
            self.logger.info(f"✅ Step {step_name} completed in {duration:.2f}s")
            await self._emit_event(
                "step_completed",
                extra={"step": step_name, "duration": duration}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            self.metrics[f"{step_name}_duration"] = duration
            self.metrics[f"{step_name}_failed"] = True
            await self._emit_event(
                "step_failed",
                extra={"step": step_name, "duration": duration, "error": str(e)}
            )
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
        await self._emit_event("setup_started")
        
        # Create the connector instance using the registry
        from monke.bongos.registry import BongoRegistry
        from monke.auth.credentials_resolver import resolve_credentials
        
        try:
            # Create bongo instance
            resolved_creds = await resolve_credentials(
                self.config.connector.type, self.config.connector.auth_fields
            )
            bongo = BongoRegistry.create(
                self.config.connector.type,
                resolved_creds,
                entity_count=self.config.entity_count,
                **self.config.connector.config_fields
            )
            
            # Store bongo in config for steps to access
            self.config._bongo = bongo
            
            # Create Airweave SDK client (requires `pip install airweave-sdk`)
            from airweave import AirweaveSDK
            import os as _os
            airweave_client = AirweaveSDK(
                base_url=_os.getenv("AIRWEAVE_API_URL", "http://localhost:8000"),
                api_key=_os.getenv("AIRWEAVE_API_KEY"),
            )
            self.config._airweave_client = airweave_client
            
            # Set up collection and source connection
            await self._setup_infrastructure(bongo, airweave_client)
            
            self.logger.info("✅ Test environment setup completed")
            await self._emit_event("setup_completed")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup test environment: {e}")
            await self._emit_event("setup_failed", extra={"error": str(e)})
            return False
    
    async def _setup_infrastructure(self, bongo, airweave_client):
        """Set up Airweave infrastructure."""
        # Create collection
        collection_name = f"monke-{self.config.connector.type}-test-{int(time.time())}"
        collection = airweave_client.collections.create_collection(name=collection_name)
        self.config._collection_id = collection.id
        self.config._collection_readable_id = collection.readable_id
        
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

        # Create source connection using keyword args per SDK signature
        if use_provider:
            auth_provider_id = os.getenv("DM_AUTH_PROVIDER_ID")
            if not auth_provider_id:
                self.logger.warning("Auth provider requested but AIRWEAVE_AUTH_PROVIDER_ID not set; falling back to explicit auth_fields if provided")
                source_connection = airweave_client.source_connections.create_source_connection(
                    name=f"{self.config.connector.type.title()} Test Connection {int(time.time())}",
                    short_name=self.config.connector.type,
                    collection=self.config._collection_readable_id,
                    auth_fields=bongo.credentials if hasattr(bongo, 'credentials') else self.config.connector.auth_fields,
                    config_fields=self.config.connector.config_fields,
                )
            else:
                src_upper = self.config.connector.type.upper()
                auth_config_id = os.getenv(f"{src_upper}_AUTH_PROVIDER_AUTH_CONFIG_ID")
                account_id = os.getenv(f"{src_upper}_AUTH_PROVIDER_ACCOUNT_ID")
                auth_provider_config = None
                if auth_config_id and account_id:
                    auth_provider_config = {"auth_config_id": auth_config_id, "account_id": account_id}

                kwargs = dict(
                    name=f"{self.config.connector.type.title()} Test Connection {int(time.time())}",
                    short_name=self.config.connector.type,
                    collection=self.config._collection_readable_id,
                    auth_provider=auth_provider_id,
                )
                if auth_provider_config:
                    kwargs["auth_provider_config"] = auth_provider_config

                source_connection = airweave_client.source_connections.create_source_connection(**kwargs)
        else:
            source_connection = airweave_client.source_connections.create_source_connection(
                name=f"{self.config.connector.type.title()} Test Connection {int(time.time())}",
                short_name=self.config.connector.type,
                collection=self.config._collection_readable_id,
                auth_fields=self.config.connector.auth_fields,
                config_fields=self.config.connector.config_fields,
            )

        # Track id for cleanup (pydantic model field)
        self.config._source_connection_id = source_connection.id
    
    async def cleanup(self) -> bool:
        """Clean up the test environment."""
        try:
            self.logger.info("🧹 Cleaning up test environment")
            await self._emit_event("cleanup_started")
            
            if hasattr(self.config, '_source_connection_id'):
                # Delete source connection
                self.config._airweave_client.source_connections.delete_source_connection(self.config._source_connection_id)
                self.logger.info("✅ Deleted source connection")
            
            if hasattr(self.config, '_collection_readable_id'):
                # Delete collection
                self.config._airweave_client.collections.delete_collection(self.config._collection_readable_id)
                self.logger.info("✅ Deleted test collection")
            
            self.logger.info("✅ Test environment cleanup completed")
            await self._emit_event("cleanup_completed")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to cleanup test environment: {e}")
            await self._emit_event("cleanup_failed", extra={"error": str(e)})
            return False

    async def _emit_event(self, event_type: str, extra: Optional[Dict[str, Any]] = None) -> None:
        payload: Dict[str, Any] = {
            "type": event_type,
            "run_id": self.run_id,
            "ts": time.time(),
            "connector": self.config.connector.type,
        }
        if extra:
            payload.update(extra)
        try:
            await events.publish(payload)
        except Exception:
            # Event bus errors must not break test flow
            pass
