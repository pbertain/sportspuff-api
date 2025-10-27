#!/usr/bin/env python3
"""
Schedule updater script for the sports data service.

This script fetches and stores game schedules for all supported leagues.
It can be run manually or via systemd timer.
"""

import sys
import os
import argparse
import logging
from datetime import datetime, date

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import settings
from services import ScheduleUpdater
from database import create_tables

# Set up logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main function for schedule updater."""
    parser = argparse.ArgumentParser(description='Update sports schedules')
    parser.add_argument('--league', help='Specific league to update (NBA, MLB, NHL, NFL, WNBA)')
    parser.add_argument('--date', help='Date to update (YYYY-MM-DD format)')
    parser.add_argument('--stats', action='store_true', help='Show schedule statistics')
    parser.add_argument('--cleanup', action='store_true', help='Clean up old data')
    parser.add_argument('--dry-run', action='store_true', help='Dry run for cleanup')
    
    args = parser.parse_args()
    
    try:
        # Initialize database tables
        create_tables()
        logger.info("Database tables initialized")
        
        # Create schedule updater
        updater = ScheduleUpdater()
        
        if args.stats:
            # Show statistics
            stats = updater.get_schedule_stats()
            print("\n📊 Schedule Statistics")
            print("=" * 50)
            
            for league, league_stats in stats.items():
                print(f"\n{league}:")
                print(f"  Total Games: {league_stats['total_games']}")
                print(f"  Scheduled: {league_stats['scheduled']}")
                print(f"  In Progress: {league_stats['in_progress']}")
                print(f"  Final: {league_stats['final']}")
                
                if league_stats['date_range'][0]:
                    print(f"  Date Range: {league_stats['date_range'][0]} to {league_stats['date_range'][1]}")
        
        elif args.cleanup:
            # Clean up old data
            logger.info("Starting data cleanup...")
            results = updater.cleanup_old_data(dry_run=args.dry_run)
            
            print("\n🧹 Data Cleanup Results")
            print("=" * 30)
            
            for league, count in results.items():
                action = "Would delete" if args.dry_run else "Deleted"
                print(f"{league}: {action} {count} old games")
        
        else:
            # Update schedules
            target_date = None
            if args.date:
                try:
                    target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
                except ValueError:
                    logger.error(f"Invalid date format: {args.date}")
                    return 1
            
            if args.league:
                # Update specific league
                logger.info(f"Updating {args.league} schedule")
                count = updater.update_league(args.league, target_date)
                print(f"✅ Updated {args.league}: {count} games")
            else:
                # Update all leagues
                logger.info("Updating all league schedules")
                results = updater.update_all_leagues(target_date)
                
                print("\n📅 Schedule Update Results")
                print("=" * 30)
                
                total_games = 0
                for league, count in results.items():
                    print(f"{league}: {count} games")
                    total_games += count
                
                print(f"\nTotal: {total_games} games updated")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error in schedule updater: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
