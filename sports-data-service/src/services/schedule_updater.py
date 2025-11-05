"""
Schedule updater service for fetching and storing game schedules.
"""

import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
from sqlalchemy.orm import Session

from config import settings
from database import get_db_session
from models import Game
from collectors import NBACollector, MLBCollector, NHLCollector, NFLCollector, WNBACollector
from utils import api_tracker

logger = logging.getLogger(__name__)


class ScheduleUpdater:
    """Updates game schedules for all leagues."""
    
    def __init__(self):
        self.collectors = {
            'NBA': NBACollector(),
            'MLB': MLBCollector(),
            'NHL': NHLCollector(),
            'NFL': NFLCollector(),
            'WNBA': WNBACollector(),
        }
    
    def update_all_leagues(self, target_date: date = None) -> Dict[str, int]:
        """
        Update schedules for all leagues.
        
        Args:
            target_date: Date to update schedules for (optional, defaults to today)
            
        Returns:
            Dictionary mapping league to number of games updated
        """
        if target_date is None:
            target_date = datetime.now().date()
        
        results = {}
        
        for league, collector in self.collectors.items():
            try:
                logger.info(f"Updating {league} schedule for {target_date}")
                
                # Check if we can make API requests
                if not api_tracker.can_make_request(league):
                    wait_time = api_tracker.get_wait_time(league)
                    logger.warning(f"Rate limit reached for {league}, waiting {wait_time:.1f}s")
                    continue
                
                # Fetch schedule
                games = collector.get_schedule(target_date)
                
                # Record API usage
                api_tracker.record_request(league, 'schedule', success=True)
                
                # Store games in database
                stored_count = self._store_games(games, league)
                results[league] = stored_count
                
                logger.info(f"Updated {league}: {stored_count} games stored")
                
            except Exception as e:
                logger.error(f"Error updating {league} schedule: {e}")
                api_tracker.record_request(league, 'schedule', success=False, error_message=str(e))
                results[league] = 0
        
        return results
    
    def update_league(self, league: str, target_date: date = None, days_ahead: int = 0) -> int:
        """
        Update schedule for a specific league.
        
        Args:
            league: League identifier
            target_date: Date to update schedule for (optional, defaults to today)
            days_ahead: Number of days ahead to fetch (0 = just target_date, 14 = target_date + 14 days)
            
        Returns:
            Number of games updated
        """
        if target_date is None:
            target_date = datetime.now().date()
        
        if league not in self.collectors:
            logger.error(f"Unknown league: {league}")
            return 0
        
        total_stored = 0
        
        # Fetch for target_date and days_ahead
        for day_offset in range(days_ahead + 1):
            current_date = target_date + timedelta(days=day_offset)
            
            try:
                collector = self.collectors[league]
                logger.info(f"Updating {league} schedule for {current_date}")
                
                # Check rate limits
                if not api_tracker.can_make_request(league):
                    wait_time = api_tracker.get_wait_time(league)
                    logger.warning(f"Rate limit reached for {league}, waiting {wait_time:.1f}s")
                    continue
                
                # Fetch schedule
                games = collector.get_schedule(current_date)
                
                # Record API usage
                api_tracker.record_request(league, 'schedule', success=True)
                
                # Store games in database
                stored_count = self._store_games(games, league)
                total_stored += stored_count
                
                logger.info(f"Updated {league} for {current_date}: {stored_count} games stored")
                
            except Exception as e:
                logger.error(f"Error updating {league} schedule for {current_date}: {e}")
                api_tracker.record_request(league, 'schedule', success=False, error_message=str(e))
                continue
        
        logger.info(f"Updated {league}: {total_stored} total games stored")
        return total_stored
    
    def _store_games(self, games: List[Dict[str, Any]], league: str) -> int:
        """
        Store games in the database.
        
        Args:
            games: List of game data dictionaries
            league: League identifier
            
        Returns:
            Number of games stored
        """
        stored_count = 0
        
        with get_db_session() as db:
            for game_data in games:
                try:
                    # Ensure league is set
                    game_data['league'] = league
                    
                    # Upsert game
                    collector = self.collectors[league]
                    collector.upsert_game(db, game_data)
                    stored_count += 1
                    
                except Exception as e:
                    logger.error(f"Error storing game {game_data.get('game_id', 'unknown')}: {e}")
                    continue
        
        return stored_count
    
    def get_schedule_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Get schedule statistics for all leagues.
        
        Returns:
            Dictionary with schedule stats per league
        """
        stats = {}
        
        with get_db_session() as db:
            for league in self.collectors.keys():
                # Get total games
                total_games = db.query(Game).filter(Game.league == league).count()
                
                # Get games by status
                scheduled = db.query(Game).filter(
                    Game.league == league,
                    Game.game_status == 'scheduled'
                ).count()
                
                in_progress = db.query(Game).filter(
                    Game.league == league,
                    Game.game_status == 'in_progress'
                ).count()
                
                final = db.query(Game).filter(
                    Game.league == league,
                    Game.game_status == 'final'
                ).count()
                
                # Get date range
                date_range = db.query(
                    db.func.min(Game.game_date),
                    db.func.max(Game.game_date)
                ).filter(Game.league == league).first()
                
                stats[league] = {
                    'total_games': total_games,
                    'scheduled': scheduled,
                    'in_progress': in_progress,
                    'final': final,
                    'date_range': date_range
                }
        
        return stats
    
    def update_season_schedule(self, league: str = None, season: str = None) -> Dict[str, int]:
        """
        Update full season schedule for league(s).
        
        Args:
            league: League identifier (optional, if None updates all leagues)
            season: Season identifier (optional, will auto-detect if None)
            
        Returns:
            Dictionary mapping league to number of games stored
        """
        results = {}
        
        leagues_to_update = [league] if league else list(self.collectors.keys())
        
        for league_name in leagues_to_update:
            if league_name not in self.collectors:
                logger.error(f"Unknown league: {league_name}")
                results[league_name] = 0
                continue
                
            try:
                collector = self.collectors[league_name]
                logger.info(f"Fetching full season schedule for {league_name}")
                
                # Check rate limits
                if not api_tracker.can_make_request(league_name):
                    wait_time = api_tracker.get_wait_time(league_name)
                    logger.warning(f"Rate limit reached for {league_name}, waiting {wait_time:.1f}s")
                    results[league_name] = 0
                    continue
                
                # Fetch full season schedule
                games = collector.get_season_schedule(season=season)
                
                # Record API usage
                api_tracker.record_request(league_name, 'season_schedule', success=True)
                
                # Store games in database
                stored_count = self._store_games(games, league_name)
                results[league_name] = stored_count
                
                logger.info(f"Updated {league_name} season: {stored_count} games stored")
                
            except Exception as e:
                logger.error(f"Error updating {league_name} season schedule: {e}")
                api_tracker.record_request(league_name, 'season_schedule', success=False, error_message=str(e))
                results[league_name] = 0
        
        return results
    
    def cleanup_old_data(self, dry_run: bool = True) -> Dict[str, int]:
        """
        Clean up old season data based on configured retention policy.
        
        Args:
            dry_run: If True, only report what would be deleted
            
        Returns:
            Dictionary mapping league to number of games that would be deleted
        """
        if not settings.cleanup_old_seasons:
            logger.info("Data cleanup is disabled")
            return {}
        
        results = {}
        
        with get_db_session() as db:
            for league in self.collectors.keys():
                # Get games older than 2 seasons
                # This is a simplified cleanup - you might want more sophisticated logic
                cutoff_date = datetime.now().date() - timedelta(days=730)  # 2 years
                
                old_games = db.query(Game).filter(
                    Game.league == league,
                    Game.game_date < cutoff_date
                ).count()
                
                if not dry_run and old_games > 0:
                    # Actually delete the games
                    deleted = db.query(Game).filter(
                        Game.league == league,
                        Game.game_date < cutoff_date
                    ).delete()
                    db.commit()
                    logger.info(f"Deleted {deleted} old games for {league}")
                
                results[league] = old_games
        
        return results
