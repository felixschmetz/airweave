#!/usr/bin/env python3
"""Test Asana integration using the regular datamonkey test runner.

This uses the same test runner infrastructure as other connectors.

Environment setup options:
1. Direct credentials:
   - ASANA_ACCESS_TOKEN

2. Via auth provider (e.g., Composio):
   - DM_AUTH_PROVIDER=composio
   - DM_AUTH_PROVIDER_API_KEY=comp_xxx
   - DM_AUTH_PROVIDER_AUTH_CONFIG_ID=ic_yyy (optional)
   - DM_AUTH_PROVIDER_ACCOUNT_ID=acc_zzz (optional)

3. Airweave API:
   - AIRWEAVE_API_URL (defaults to http://localhost:8000)
"""

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
    """Run Asana test using the standard test runner."""
    # Load environment variables
    env_file = current_dir / "env.test"
    if env_file.exists():
        load_dotenv(env_file)
        print(f"✅ Loaded environment from {env_file}")
    else:
        print(f"⚠️ Environment file not found: {env_file}")
        print("Using system environment variables")
    
    # Get logger
    logger = get_logger("test_asana_integration")
    
    # Validate that we have credentials configured (either direct or via provider)
    has_direct_creds = bool(os.getenv("ASANA_ACCESS_TOKEN"))
    has_provider = bool(os.getenv("DM_AUTH_PROVIDER") and os.getenv("DM_AUTH_PROVIDER_API_KEY"))
    
    if not has_direct_creds and not has_provider:
        logger.error("❌ No Asana credentials configured!")
        logger.error("Please set either:")
        logger.error("  1. ASANA_ACCESS_TOKEN for direct authentication")
        logger.error("  2. DM_AUTH_PROVIDER + DM_AUTH_PROVIDER_API_KEY for provider-based auth")
        sys.exit(1)
    
    if has_provider:
        logger.info(f"🔐 Using auth provider: {os.getenv('DM_AUTH_PROVIDER')}")
    else:
        logger.info("🔑 Using direct credentials (ASANA_ACCESS_TOKEN)")
    
    logger.info("🚀 Testing Asana integration with datamonkey")
    
    try:
        # Use the Asana test configuration
        config_path = current_dir / "configs" / "asana_test.yaml"
        
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
