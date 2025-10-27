#!/usr/bin/env python3
"""
Test script for the sports data service.

This script tests the basic functionality of the service.
"""

import sys
import os
import logging
from datetime import datetime, date

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import settings
from services import ScheduleUpdater, LivePoller
from database import create_tables

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_database_connection():
    """Test database connection and table creation."""
    try:
        create_tables()
        logger.info("✅ Database connection and table creation successful")
        return True
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False


def test_schedule_updater():
    """Test schedule updater functionality."""
    try:
        updater = ScheduleUpdater()
        
        # Test NBA schedule update
        logger.info("Testing NBA schedule update...")
        count = updater.update_league('NBA')
        logger.info(f"✅ NBA schedule update successful: {count} games")
        
        # Test statistics
        stats = updater.get_schedule_stats()
        logger.info(f"✅ Schedule statistics: {stats}")
        
        return True
    except Exception as e:
        logger.error(f"❌ Schedule updater test failed: {e}")
        return False


def test_live_poller():
    """Test live poller functionality."""
    try:
        poller = LivePoller()
        
        # Test polling status
        status = poller.get_polling_status()
        logger.info(f"✅ Live poller status: {status}")
        
        # Test one-time poll
        logger.info("Testing one-time live score poll...")
        results = poller.poll_once(['NBA'])
        logger.info(f"✅ Live score poll successful: {results}")
        
        return True
    except Exception as e:
        logger.error(f"❌ Live poller test failed: {e}")
        return False


def test_configuration():
    """Test configuration loading."""
    try:
        logger.info(f"✅ Configuration loaded successfully")
        logger.info(f"  Database URL: {settings.database_url}")
        logger.info(f"  Log Level: {settings.log_level}")
        logger.info(f"  NBA Close Game Threshold: {settings.nba_close_game_threshold}")
        logger.info(f"  Schedule Update Times: {settings.schedule_update_times_list}")
        logger.info(f"  Live Polling Hours: {settings.live_polling_hours_list}")
        return True
    except Exception as e:
        logger.error(f"❌ Configuration test failed: {e}")
        return False


def main():
    """Run all tests."""
    logger.info("🧪 Starting Sports Data Service Tests")
    logger.info("=" * 50)
    
    tests = [
        ("Configuration", test_configuration),
        ("Database Connection", test_database_connection),
        ("Schedule Updater", test_schedule_updater),
        ("Live Poller", test_live_poller),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n🔍 Running {test_name} test...")
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"❌ {test_name} test crashed: {e}")
            results[test_name] = False
    
    # Summary
    logger.info("\n📊 Test Results Summary")
    logger.info("=" * 30)
    
    passed = 0
    total = len(tests)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Service is ready to use.")
        return 0
    else:
        logger.error("💥 Some tests failed. Please check the logs.")
        return 1


if __name__ == '__main__':
    sys.exit(main())
