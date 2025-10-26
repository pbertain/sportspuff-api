"""
API usage tracking utilities.
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from collections import defaultdict, deque

from ..config import settings
from ..models import ApiUsage
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class APITracker:
    """Tracks API usage and enforces rate limits."""
    
    def __init__(self):
        self.request_history: Dict[str, deque] = defaultdict(lambda: deque())
        self.daily_usage: Dict[str, int] = defaultdict(int)
        self.last_reset = datetime.now().date()
    
    def can_make_request(self, league: str) -> bool:
        """
        Check if we can make a request without exceeding rate limits.
        
        Args:
            league: League identifier
            
        Returns:
            True if request is allowed
        """
        self._cleanup_old_requests()
        
        max_requests = settings.get_max_requests_per_minute(league)
        current_requests = len(self.request_history[league])
        
        return current_requests < max_requests
    
    def record_request(self, league: str, endpoint: str, success: bool = True, 
                      response_time_ms: Optional[int] = None, error_message: Optional[str] = None):
        """
        Record an API request.
        
        Args:
            league: League identifier
            endpoint: API endpoint called
            success: Whether the request was successful
            response_time_ms: Response time in milliseconds
            error_message: Error message if request failed
        """
        now = time.time()
        self.request_history[league].append(now)
        self.daily_usage[league] += 1
        
        logger.debug(f"Recorded {league} API request to {endpoint}: success={success}")
    
    def log_to_database(self, db: Session, league: str, endpoint: str, success: bool = True,
                       response_time_ms: Optional[int] = None, error_message: Optional[str] = None):
        """
        Log API usage to the database.
        
        Args:
            db: Database session
            league: League identifier
            endpoint: API endpoint called
            success: Whether the request was successful
            response_time_ms: Response time in milliseconds
            error_message: Error message if request failed
        """
        try:
            api_usage = ApiUsage(
                league=league,
                endpoint=endpoint,
                success=success,
                error_message=error_message,
                response_time_ms=response_time_ms
            )
            db.add(api_usage)
            db.commit()
        except Exception as e:
            logger.error(f"Failed to log API usage to database: {e}")
            db.rollback()
    
    def get_wait_time(self, league: str) -> float:
        """
        Get the time to wait before next request.
        
        Args:
            league: League identifier
            
        Returns:
            Seconds to wait, or 0 if no wait needed
        """
        self._cleanup_old_requests()
        
        max_requests = settings.get_max_requests_per_minute(league)
        current_requests = len(self.request_history[league])
        
        if current_requests < max_requests:
            return 0
        
        # Calculate wait time until oldest request expires
        oldest_request = self.request_history[league][0]
        wait_time = 60 - (time.time() - oldest_request)
        
        return max(0, wait_time)
    
    def get_daily_usage(self, league: str) -> int:
        """
        Get daily usage count for a league.
        
        Args:
            league: League identifier
            
        Returns:
            Number of requests made today
        """
        self._reset_daily_usage_if_needed()
        return self.daily_usage[league]
    
    def get_usage_stats(self) -> Dict[str, Dict[str, int]]:
        """
        Get usage statistics for all leagues.
        
        Returns:
            Dictionary with usage stats per league
        """
        self._cleanup_old_requests()
        self._reset_daily_usage_if_needed()
        
        stats = {}
        for league in ['NBA', 'MLB', 'NHL', 'NFL', 'WNBA']:
            stats[league] = {
                'requests_last_minute': len(self.request_history[league]),
                'requests_today': self.daily_usage[league],
                'max_per_minute': settings.get_max_requests_per_minute(league)
            }
        
        return stats
    
    def _cleanup_old_requests(self):
        """Remove requests older than 1 minute."""
        now = time.time()
        cutoff = now - 60
        
        for league in self.request_history:
            while self.request_history[league] and self.request_history[league][0] < cutoff:
                self.request_history[league].popleft()
    
    def _reset_daily_usage_if_needed(self):
        """Reset daily usage if it's a new day."""
        today = datetime.now().date()
        if today > self.last_reset:
            self.daily_usage.clear()
            self.last_reset = today
            logger.info("Reset daily API usage counters")


class APIMonitor:
    """Monitors API usage and provides alerts."""
    
    def __init__(self):
        self.tracker = APITracker()
        self.alerts_sent = set()
    
    def check_rate_limits(self) -> Dict[str, bool]:
        """
        Check if any leagues are approaching rate limits.
        
        Returns:
            Dictionary mapping league to whether it's approaching limits
        """
        stats = self.tracker.get_usage_stats()
        approaching_limits = {}
        
        for league, stats_data in stats.items():
            requests_last_minute = stats_data['requests_last_minute']
            max_per_minute = stats_data['max_per_minute']
            
            # Consider approaching limit if > 80% of max
            approaching_limits[league] = requests_last_minute > (max_per_minute * 0.8)
        
        return approaching_limits
    
    def send_alert_if_needed(self, league: str, usage_percent: float):
        """
        Send alert if usage is high.
        
        Args:
            league: League identifier
            usage_percent: Usage percentage (0-100)
        """
        if usage_percent > 90 and league not in self.alerts_sent:
            logger.warning(f"HIGH API USAGE ALERT: {league} at {usage_percent:.1f}% of rate limit")
            self.alerts_sent.add(league)
        elif usage_percent < 70 and league in self.alerts_sent:
            # Reset alert if usage drops
            self.alerts_sent.discard(league)
    
    def get_recommendations(self) -> List[str]:
        """
        Get recommendations for API usage optimization.
        
        Returns:
            List of recommendation strings
        """
        recommendations = []
        stats = self.tracker.get_usage_stats()
        
        for league, stats_data in stats.items():
            requests_today = stats_data['requests_today']
            requests_last_minute = stats_data['requests_last_minute']
            max_per_minute = stats_data['max_per_minute']
            
            if requests_last_minute > max_per_minute * 0.8:
                recommendations.append(f"Consider reducing polling frequency for {league}")
            
            if requests_today > 1000:  # Arbitrary threshold
                recommendations.append(f"High daily usage for {league}: {requests_today} requests")
        
        return recommendations


# Global instances
api_tracker = APITracker()
api_monitor = APIMonitor()
