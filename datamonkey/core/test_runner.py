"""Core test runner that orchestrates the entire testing process."""

from typing import Any, Dict, List
from dataclasses import dataclass

from datamonkey.core.test_config import TestConfig
from datamonkey.core.test_flow import TestFlow
from datamonkey.utils.logging import get_logger


@dataclass
class TestResult:
    """Result of a test execution."""
    success: bool
    duration: float
    metrics: Dict[str, Any]
    errors: List[str]
    warnings: List[str]


class TestRunner:
    """Main test runner that orchestrates the entire testing process."""
    
    def __init__(self, config_path: str):
        """Initialize the test runner.
        
        Args:
            config_path: Path to the test configuration file
        """
        self.config = TestConfig.from_file(config_path)
        self.logger = get_logger("test_runner")
        
    async def run_tests(self) -> List[TestResult]:
        """Run all configured tests."""
        self.logger.info(f"🚀 Starting test run: {self.config.name}")
        self.logger.info(f"📝 Description: {self.config.description}")
        self.logger.info(f"🔗 Connector: {self.config.connector.type}")
        
        test_flow = None
        try:
            # Use the new TestFlow system for all connectors, including GitHub
            # This ensures the YAML test_flow steps are properly executed step-by-step
            test_flow = TestFlow.create(self.config)
            
            # Set up the test environment
            if not await test_flow.setup():
                raise Exception("Failed to setup test environment")
            
            # Execute the test flow
            await test_flow.execute()
            
            # Clean up the test environment
            await test_flow.cleanup()
            
            # Create test result
            result = TestResult(
                success=True,
                duration=sum(test_flow.get_metrics().values()),
                metrics=test_flow.get_metrics(),
                errors=[],
                warnings=test_flow.get_warnings()
            )
            
            return [result]
            
        except Exception as e:
            self.logger.error(f"❌ Test execution failed: {e}")
            
            # Ensure cleanup happens even on failure
            if test_flow:
                try:
                    self.logger.info("🧹 Attempting emergency cleanup after test failure...")
                    await test_flow.cleanup()
                    self.logger.info("✅ Emergency cleanup completed")
                except Exception as cleanup_error:
                    self.logger.error(f"❌ Emergency cleanup failed: {cleanup_error}")
            
            result = TestResult(
                success=False,
                duration=0.0,
                metrics={},
                errors=[str(e)],
                warnings=[]
            )
            
            return [result]
