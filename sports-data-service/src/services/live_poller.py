"""
Live score polling service for real-time game updates.
"""

import time
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from config import settings
from database import get_db_session
from models import Game
from collectors import NBACollector, MLBCollector, NHLCollector, NFLCollector, WNBACollector
from utils import AdaptivePollingManager, api_tracker

logger = logging.getLogger(__name__)


class LivePoller:
    """Polls live scores for active games."""
    
    def __init__(self):
        self.collectors = {
            'NBA': NBACollector(),
            'MLB': MLBCollector(),
            'NHL': NHLCollector(),
            'NFL': NFLCollector(),
            'WNBA': WNBACollector(),
        }
        self.polling_manager = AdaptivePollingManager()
        self.is_running = False
    
    def start_polling(self, leagues: Optional[List[str]] = None):
        """
        Start polling live scores.
        
        Args:
            leagues: List of leagues to poll (optional, defaults to all)
        """
        if leagues is None:
            leagues = list(self.collectors.keys())
        
        self.is_running = True
        logger.info(f"Starting live polling for leagues: {leagues}")
        
        try:
            while self.is_running:
                # Check if we should poll based on time
                if not self.polling_manager.should_poll_now():
                    logger.debug("Outside polling hours, sleeping for 5 minutes")
                    time.sleep(300)  # Sleep for 5 minutes
                    continue
                
                # Poll each league
                for league in leagues:
                    if not self.is_running:
                        break
                    
                    try:
                        self._poll_league(league)
                    except Exception as e:
                        logger.error(f"Error polling {league}: {e}")
                
                # Determine next poll interval
                next_interval = self._get_next_poll_interval(leagues)
                if next_interval is None:
                    logger.info("No active games, stopping polling")
                    break
                
                logger.debug(f"Sleeping for {next_interval} seconds")
                time.sleep(next_interval)
                
        except KeyboardInterrupt:
            logger.info("Polling interrupted by user")
        finally:
            self.is_running = False
            logger.info("Live polling stopped")
    
    def stop_polling(self):
        """Stop the polling process."""
        self.is_running = False
        logger.info("Stopping live polling...")
    
    def poll_once(self, leagues: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Poll live scores once for specified leagues.
        
        Args:
            leagues: List of leagues to poll (optional, defaults to all)
            
        Returns:
            Dictionary mapping league to number of games updated
        """
        if leagues is None:
            leagues = list(self.collectors.keys())
        
        results = {}
        
        for league in leagues:
            try:
                updated_count = self._poll_league(league)
                results[league] = updated_count
            except Exception as e:
                logger.error(f"Error polling {league}: {e}")
                results[league] = 0
        
        return results
    
    def _poll_league(self, league: str) -> int:
        """
        Poll live scores for a specific league.
        
        Args:
            league: League identifier
            
        Returns:
            Number of games updated
        """
        # Check if we should poll this league today
        with get_db_session() as db:
            if not self.polling_manager.should_poll_today(db):
                logger.debug(f"No games scheduled today for {league}")
                return 0
        
        # Check rate limits
        if not api_tracker.can_make_request(league):
            wait_time = api_tracker.get_wait_time(league)
            logger.warning(f"Rate limit reached for {league}, waiting {wait_time:.1f}s")
            return 0
        
        try:
            collector = self.collectors[league]
            
            # Get live scores
            live_games = collector.get_live_scores()
            
            # Record API usage
            api_tracker.record_request(league, 'live_scores', success=True)
            
            # Update games in database
            updated_count = self._update_live_games(live_games, league)
            
            logger.debug(f"Updated {updated_count} live games for {league}")
            return updated_count
            
        except Exception as e:
            logger.error(f"Error polling live scores for {league}: {e}")
            api_tracker.record_request(league, 'live_scores', success=False, error_message=str(e))
            return 0
    
    def _update_live_games(self, live_games: List[Dict[str, Any]], league: str) -> int:
        """
        Update live game data in the database.
        
        Args:
            live_games: List of live game data
            league: League identifier
            
        Returns:
            Number of games updated
        """
        updated_count = 0
        
        with get_db_session() as db:
            for game_data in live_games:
                try:
                    # Ensure league is set
                    game_data['league'] = league
                    
                    # Find existing game
                    existing_game = db.query(Game).filter(
                        Game.league == league,
                        Game.game_id == game_data['game_id']
                    ).first()
                    
                    if existing_game:
                        # Update existing game with live data
                        for key, value in game_data.items():
                            if hasattr(existing_game, key) and value is not None:
                                setattr(existing_game, key, value)
                        
                        existing_game.updated_at = datetime.utcnow()
                        db.commit()
                        updated_count += 1
                        
                        logger.debug(f"Updated live data for {league} game {game_data['game_id']}")
                    else:
                        # Create new game if it doesn't exist
                        collector = self.collectors[league]
                        collector.upsert_game(db, game_data)
                        updated_count += 1
                        
                        logger.debug(f"Created new game for {league} game {game_data['game_id']}")
                
                except Exception as e:
                    logger.error(f"Error updating live game {game_data.get('game_id', 'unknown')}: {e}")
                    continue
        
        return updated_count
    
    def _get_next_poll_interval(self, leagues: List[str]) -> Optional[int]:
        """
        Get the next polling interval based on current game states.
        
        Args:
            leagues: List of leagues being polled
            
        Returns:
            Next polling interval in seconds, or None to stop polling
        """
        with get_db_session() as db:
            # Check if any league has active games
            has_active_games = False
            min_interval = None
            
            for league in leagues:
                interval = self.polling_manager.determine_poll_interval(db, league)
                if interval is not None:
                    has_active_games = True
                    if min_interval is None or interval < min_interval:
                        min_interval = interval
            
            return min_interval if has_active_games else None
    
    def get_polling_status(self) -> Dict[str, Any]:
        """
        Get current polling status.
        
        Returns:
            Dictionary with polling status information
        """
        with get_db_session() as db:
            status = {
                'is_running': self.is_running,
                'should_poll_now': self.polling_manager.should_poll_now(),
                'should_poll_today': self.polling_manager.should_poll_today(db),
                'leagues': {}
            }
            
            for league in self.collectors.keys():
                games = self.polling_manager.get_games_to_poll(db, league)
                status['leagues'][league] = {
                    'active_games': len(games),
                    'games': [
                        {
                            'game_id': game.game_id,
                            'status': game.game_status,
                            'home_score': game.home_score_total,
                            'visitor_score': game.visitor_score_total,
                            'is_final': game.is_final
                        }
                        for game in games
                    ]
                }
            
            return status
    
    def force_update_all(self) -> Dict[str, int]:
        """
        Force update all active games regardless of polling schedule.
        
        Returns:
            Dictionary mapping league to number of games updated
        """
        logger.info("Force updating all active games")
        return self.poll_once()
