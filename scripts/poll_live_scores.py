#!/usr/bin/env python3
"""
Live score polling script for the sports data service.

This script polls live scores for active games and can run continuously
or as a one-time update.
"""

import sys
import os
import argparse
import logging
import signal
from datetime import datetime

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import settings
from services import LivePoller
from database import create_tables

# Set up logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global poller instance for signal handling
poller = None


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global poller
    if poller:
        logger.info(f"Received signal {signum}, stopping poller...")
        poller.stop_polling()


def main():
    """Main function for live score poller."""
    global poller
    
    parser = argparse.ArgumentParser(description='Poll live sports scores')
    parser.add_argument('--league', help='Specific league to poll (NBA, MLB, NHL, NFL, WNBA)')
    parser.add_argument('--once', action='store_true', help='Poll once and exit')
    parser.add_argument('--status', action='store_true', help='Show polling status')
    parser.add_argument('--force', action='store_true', help='Force update all active games')
    
    args = parser.parse_args()
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Initialize database tables
        create_tables()
        logger.info("Database tables initialized")
        
        # Create live poller
        poller = LivePoller()
        
        if args.status:
            # Show polling status
            status = poller.get_polling_status()
            
            print("\n📊 Live Polling Status")
            print("=" * 30)
            print(f"Running: {status['is_running']}")
            print(f"Should poll now: {status['should_poll_now']}")
            print(f"Should poll today: {status['should_poll_today']}")
            
            print("\nLeagues:")
            for league, league_status in status['leagues'].items():
                print(f"  {league}: {league_status['active_games']} active games")
                
                if league_status['games']:
                    print("    Games:")
                    for game in league_status['games']:
                        status_text = f"{game['status']}"
                        if game['home_score'] is not None and game['visitor_score'] is not None:
                            status_text += f" ({game['visitor_score']}-{game['home_score']})"
                        if game['is_final']:
                            status_text += " [FINAL]"
                        print(f"      {game['game_id']}: {status_text}")
        
        elif args.force:
            # Force update all active games
            logger.info("Force updating all active games")
            results = poller.force_update_all()
            
            print("\n🔄 Force Update Results")
            print("=" * 25)
            
            total_games = 0
            for league, count in results.items():
                print(f"{league}: {count} games updated")
                total_games += count
            
            print(f"\nTotal: {total_games} games updated")
        
        elif args.once:
            # Poll once and exit
            leagues = [args.league] if args.league else None
            logger.info("Polling live scores once")
            
            results = poller.poll_once(leagues)
            
            print("\n🔄 Live Score Update Results")
            print("=" * 30)
            
            total_games = 0
            for league, count in results.items():
                print(f"{league}: {count} games updated")
                total_games += count
            
            print(f"\nTotal: {total_games} games updated")
        
        else:
            # Continuous polling
            leagues = [args.league] if args.league else None
            logger.info("Starting continuous live score polling")
            
            try:
                poller.start_polling(leagues)
            except KeyboardInterrupt:
                logger.info("Polling interrupted by user")
            finally:
                poller.stop_polling()
        
        return 0
        
    except Exception as e:
        logger.error(f"Error in live score poller: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
