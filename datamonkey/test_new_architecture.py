#!/usr/bin/env python3
"""Test script for the new datamonkey architecture."""

import asyncio
import os
import sys
from pathlib import Path

# Add the parent directory to the path so we can import datamonkey modules
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))

# Load environment variables
from dotenv import load_dotenv

# Now we can import from the datamonkey package
from datamonkey.core.test_runner import TestRunner
from datamonkey.utils.logging import get_logger


async def main():
    """Test the new datamonkey architecture."""
    # Load environment variables
    env_file = current_dir / "env.test"
    if env_file.exists():
        load_dotenv(env_file)
        print(f"✅ Loaded environment from {env_file}")
    else:
        print(f"⚠️ Environment file not found: {env_file}")
        print("Using system environment variables")
    
    # Get logger
    logger = get_logger("test_new_architecture")
    
    # Validate required environment variables
    required_vars = [
        "GITHUB_PERSONAL_ACCESS_TOKEN",
        "GITHUB_REPO_NAME"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {missing_vars}")
        logger.error("Please check your env.test file")
        sys.exit(1)
    
    logger.info("🚀 Testing new datamonkey architecture")
    
    try:
        # Test the new configuration system
        config_path = current_dir / "configs" / "github_test.yaml"
        
        if not config_path.exists():
            logger.error(f"❌ Configuration file not found: {config_path}")
            sys.exit(1)
        
        logger.info(f"📁 Using configuration: {config_path}")
        
        # Create test runner
        runner = TestRunner(str(config_path))
        
        # Run tests
        results = await runner.run_tests()
        
        # Check results
        for result in results:
            if result.success:
                logger.info("🎉 Test completed successfully!")
                logger.info(f"📊 Duration: {result.duration:.2f}s")
                if result.metrics:
                    logger.info(f"📈 Metrics: {result.metrics}")
            else:
                logger.error("❌ Test failed!")
                for error in result.errors:
                    logger.error(f"  Error: {error}")
                sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ Test failed with exception: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)
