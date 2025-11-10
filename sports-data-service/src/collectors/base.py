"""
Abstract base class for sports data collectors.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging
import time
from sqlalchemy.orm import Session

from models import Game, ApiUsage
from config import settings

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Abstract base class for all sports data collectors."""
    
    def __init__(self, league: str):
        """
        Initialize the collector.
        
        Args:
            league: League identifier (NBA, MLB, NHL, NFL, WNBA)
        """
        self.league = league.upper()
        self.api_timeout = settings.get_api_timeout(self.league)
        self.max_requests_per_minute = settings.get_max_requests_per_minute(self.league)
        self.close_game_threshold = settings.get_close_game_threshold(self.league)
        
        # Rate limiting tracking
        self.request_times: List[float] = []
        
    def _check_rate_limit(self):
        """Check and enforce rate limiting."""
        now = time.time()
        
        # Remove requests older than 1 minute
        self.request_times = [t for t in self.request_times if now - t < 60]
        
        # If we're at the limit, wait
        if len(self.request_times) >= self.max_requests_per_minute:
            sleep_time = 60 - (now - self.request_times[0])
            if sleep_time > 0:
                logger.info(f"Rate limit reached for {self.league}, sleeping for {sleep_time:.1f}s")
                time.sleep(sleep_time)
        
        # Record this request
        self.request_times.append(now)
    
    def _log_api_usage(self, db: Session, endpoint: str, success: bool, 
                      error_message: Optional[str] = None, response_time_ms: Optional[int] = None):
        """Log API usage to the database."""
        try:
            api_usage = ApiUsage(
                league=self.league,
                endpoint=endpoint,
                success=success,
                error_message=error_message,
                response_time_ms=response_time_ms
            )
            db.add(api_usage)
            db.commit()
        except Exception as e:
            logger.error(f"Failed to log API usage: {e}")
            db.rollback()
    
    @abstractmethod
    def get_schedule(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get schedule for a specific date or current games.
        
        Args:
            date: Date to get schedule for (optional, defaults to today)
            
        Returns:
            List of game dictionaries
        """
        pass
    
    def get_season_schedule(self, season: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get full season schedule for the league.
        
        Args:
            season: Season identifier (e.g., "2024-25" for NBA, "2024" for others)
                   If None, will determine current season automatically.
            
        Returns:
            List of game dictionaries for the entire season
        """
        # Default implementation: fetch games day by day for the season
        # Individual collectors can override with more efficient methods
        logger.warning(f"get_season_schedule() not implemented for {self.league}, using date-by-date fallback")
        return []
    
    @abstractmethod
    def get_live_scores(self, date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get live scores for a specific date.
        
        Args:
            date: Date to get scores for (optional, defaults to today)
            
        Returns:
            List of game dictionaries with live score data
        """
        pass
    
    @abstractmethod
    def parse_game_data(self, raw_game: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw game data from API into standardized format.
        
        Args:
            raw_game: Raw game data from API
            
        Returns:
            Standardized game dictionary
        """
        pass
    
    def normalize_game_type(self, game_type: str) -> str:
        """
        Normalize game type to standard values.
        
        Args:
            game_type: Raw game type from API
            
        Returns:
            Normalized game type
        """
        game_type_lower = game_type.lower()
        
        # Map various game types to standard values
        if 'preseason' in game_type_lower or 'pre' in game_type_lower:
            return 'preseason'
        elif 'regular' in game_type_lower or game_type_lower == '':
            return 'regular'
        elif 'playoff' in game_type_lower or 'post' in game_type_lower:
            return 'playoffs'
        elif 'allstar' in game_type_lower or 'all-star' in game_type_lower:
            return 'allstar'
        elif 'cup' in game_type_lower or 'tournament' in game_type_lower:
            return 'nba_cup'
        else:
            return 'regular'
    
    def normalize_game_status(self, status: str) -> str:
        """
        Normalize game status to standard values.
        
        Args:
            status: Raw game status from API
            
        Returns:
            Normalized game status
        """
        status_lower = status.lower().strip()
        
        if status_lower in ['final', 'completed', 'finished', 'off']:
            return 'final'
        elif status_lower in ['live', 'in progress', 'in_progress', 'active', 'halftime']:
            return 'in_progress'
        elif status_lower in ['scheduled', 'upcoming', 'pre']:
            return 'scheduled'
        elif status_lower in ['postponed', 'delayed', 'cancelled']:
            return 'postponed'
        else:
            return 'scheduled'
    
    def is_close_game(self, home_score: int, visitor_score: int) -> bool:
        """
        Determine if a game is close based on score difference.
        
        Args:
            home_score: Home team score
            visitor_score: Visitor team score
            
        Returns:
            True if game is close
        """
        score_diff = abs(home_score - visitor_score)
        return score_diff <= self.close_game_threshold
    
    def determine_poll_interval(self, game_status: str, home_score: int, visitor_score: int) -> Optional[int]:
        """
        Determine polling interval based on game state.
        
        Args:
            game_status: Current game status
            home_score: Home team score
            visitor_score: Visitor team score
            
        Returns:
            Polling interval in seconds, or None to stop polling
        """
        if game_status == 'final':
            return None  # Stop polling
        elif game_status == 'scheduled':
            return settings.scheduled_game_poll_interval
        elif game_status == 'in_progress':
            if self.is_close_game(home_score, visitor_score):
                return settings.close_game_poll_interval
            else:
                return settings.default_poll_interval
        return settings.default_poll_interval
    
    def upsert_game(self, db: Session, game_data: Dict[str, Any]) -> Game:
        """
        Insert or update a game in the database.
        
        Args:
            db: Database session
            game_data: Game data dictionary
            
        Returns:
            Game model instance
        """
        # Try to find existing game
        existing_game = db.query(Game).filter(
            Game.league == self.league,
            Game.game_id == game_data['game_id']
        ).first()
        
        if existing_game:
            # Update existing game
            for key, value in game_data.items():
                if hasattr(existing_game, key) and value is not None:
                    setattr(existing_game, key, value)
            existing_game.updated_at = datetime.utcnow()
            db.commit()
            return existing_game
        else:
            # Create new game
            new_game = Game(**game_data)
            db.add(new_game)
            db.commit()
            db.refresh(new_game)
            return new_game
