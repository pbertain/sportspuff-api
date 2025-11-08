"""
Adaptive polling utilities for live score updates.
"""

from datetime import datetime, time
from typing import Optional, List, Dict, Any
import logging

from config import settings
from models import Game
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class AdaptivePollingManager:
    """Manages adaptive polling for live score updates."""
    
    def __init__(self):
        self.polling_active = False
        self.current_interval = settings.default_poll_interval
    
    def should_poll_today(self, db: Session) -> bool:
        """
        Check if there are any games scheduled today.
        Returns False if no games, saving API calls.
        
        Args:
            db: Database session
            
        Returns:
            True if there are games to poll, False otherwise
        """
        today = datetime.now().date()
        
        games = db.query(Game).filter(
            Game.game_date == today,
            Game.is_final == False
        ).all()
        
        has_games = len(games) > 0
        logger.info(f"Games scheduled today: {len(games)}")
        
        return has_games
    
    def should_poll_now(self) -> bool:
        """
        Check if we should poll based on configured polling hours.
        
        Returns:
            True if we should poll now, False otherwise
        """
        now = datetime.now().time()
        
        # Parse polling hours (e.g., "12:00-02:00")
        for hours_range in settings.live_polling_hours_list:
            try:
                start_str, end_str = hours_range.split('-')
                start_time = datetime.strptime(start_str.strip(), '%H:%M').time()
                end_time = datetime.strptime(end_str.strip(), '%H:%M').time()
                
                # Handle overnight ranges (e.g., 12:00-02:00)
                if start_time > end_time:
                    if now >= start_time or now <= end_time:
                        return True
                else:
                    if start_time <= now <= end_time:
                        return True
            except ValueError:
                logger.warning(f"Invalid polling hours format: {hours_range}")
                continue
        
        return False
    
    def determine_poll_interval(self, db: Session, league: str) -> Optional[int]:
        """
        Determine polling interval based on current game states.
        
        Args:
            db: Database session
            league: League to check
            
        Returns:
            Polling interval in seconds, or None to stop polling
        """
        today = datetime.now().date()
        
        # Get active games for the league
        active_games = db.query(Game).filter(
            Game.league == league,
            Game.game_date == today,
            Game.is_final == False
        ).all()
        
        if not active_games:
            return None  # No active games, stop polling
        
        # Check if all games are final
        all_final = all(game.is_final for game in active_games)
        if all_final:
            return None  # All games final, stop polling
        
        # Check for close games
        close_games = []
        for game in active_games:
            if game.game_status == 'in_progress':
                if game.home_score_total is not None and game.visitor_score_total is not None:
                    score_diff = abs(game.home_score_total - game.visitor_score_total)
                    threshold = settings.get_close_game_threshold(league)
                    if score_diff <= threshold:
                        close_games.append(game)
        
        # Determine interval based on game states
        if close_games:
            # Close games in progress - poll more frequently
            return settings.close_game_poll_interval
        elif any(game.game_status == 'in_progress' for game in active_games):
            # Games in progress but not close
            # NBA games poll every minute during games
            if league == 'NBA':
                return settings.close_game_poll_interval  # 60 seconds for NBA
            # Normal interval for other leagues
            return settings.default_poll_interval
        else:
            # Games scheduled but not started - less frequent polling
            return settings.scheduled_game_poll_interval
    
    def get_games_to_poll(self, db: Session, league: str) -> List[Game]:
        """
        Get list of games that need polling updates.
        
        Args:
            db: Database session
            league: League to check
            
        Returns:
            List of games that need updates
        """
        today = datetime.now().date()
        
        games = db.query(Game).filter(
            Game.league == league,
            Game.game_date == today,
            Game.is_final == False
        ).all()
        
        return games
    
    def update_polling_state(self, db: Session, league: str):
        """
        Update the polling state based on current game conditions.
        
        Args:
            db: Database session
            league: League to update
        """
        interval = self.determine_poll_interval(db, league)
        
        if interval is None:
            self.polling_active = False
            logger.info(f"Stopping polling for {league} - no active games")
        else:
            self.polling_active = True
            self.current_interval = interval
            logger.info(f"Polling {league} every {interval} seconds")
    
    def get_next_poll_time(self) -> datetime:
        """
        Get the next scheduled poll time.
        
        Returns:
            Next poll time
        """
        return datetime.now().timestamp() + self.current_interval


def is_close_game(home_score: int, visitor_score: int, league: str) -> bool:
    """
    Determine if a game is close based on score difference.
    
    Args:
        home_score: Home team score
        visitor_score: Visitor team score
        league: League identifier
        
    Returns:
        True if game is close
    """
    score_diff = abs(home_score - visitor_score)
    threshold = settings.get_close_game_threshold(league)
    return score_diff <= threshold


def get_polling_hours() -> List[tuple]:
    """
    Get parsed polling hours as time tuples.
    
    Returns:
        List of (start_time, end_time) tuples
    """
    hours = []
    
    for hours_range in settings.live_polling_hours_list:
        try:
            start_str, end_str = hours_range.split('-')
            start_time = datetime.strptime(start_str.strip(), '%H:%M').time()
            end_time = datetime.strptime(end_str.strip(), '%H:%M').time()
            hours.append((start_time, end_time))
        except ValueError:
            logger.warning(f"Invalid polling hours format: {hours_range}")
            continue
    
    return hours
