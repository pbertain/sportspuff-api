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
        self.monthly_usage: Dict[str, int] = defaultdict(int)
        self.last_reset = datetime.now().date()
        self.last_monthly_reset = datetime.now().replace(day=1).date()
    
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
        
        if current_requests >= max_requests:
            return False

        if league.upper() == 'WNBA':
            now = time.time()
            requests_last_second = sum(1 for t in self.request_history[league] if now - t < 1)
            if requests_last_second >= settings.wnba_max_requests_per_second:
                return False

        return True

    def can_make_budgeted_request(self, league: str, db: Optional[Session] = None) -> bool:
        """
        Check in-process rate limits plus configured paid API budgets.

        Database-backed checks use ApiUsage so deployments and process restarts do
        not reset paid API counters.
        """
        league = league.upper()
        if not self.can_make_request(league):
            return False

        if db is None:
            return self._can_make_in_memory_budgeted_request(league)

        now = datetime.utcnow()
        hour_start = now - timedelta(hours=1)
        if league == 'NFL':
            requests_last_hour = self._count_requests_since(db, league, hour_start)
            if requests_last_hour >= settings.nfl_max_requests_per_hour:
                logger.warning(
                    "NFL hourly API budget reached: %s/%s",
                    requests_last_hour,
                    settings.nfl_max_requests_per_hour,
                )
                return False
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            requests_today = self._count_requests_since(db, league, today_start)
            if requests_today >= settings.nfl_max_requests_per_day:
                logger.warning(
                    "NFL daily API budget reached: %s/%s",
                    requests_today,
                    settings.nfl_max_requests_per_day,
                )
                return False

        if league == 'WNBA':
            requests_last_hour = self._count_requests_since(db, league, hour_start)
            if requests_last_hour >= settings.wnba_max_requests_per_hour:
                logger.warning(
                    "WNBA hourly API budget reached: %s/%s",
                    requests_last_hour,
                    settings.wnba_max_requests_per_hour,
                )
                return False
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            requests_today = self._count_requests_since(db, league, today_start)
            if requests_today >= settings.wnba_max_requests_per_day:
                logger.warning(
                    "WNBA daily API budget reached: %s/%s",
                    requests_today,
                    settings.wnba_max_requests_per_day,
                )
                return False
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            requests_this_month = self._count_requests_since(db, league, month_start)
            if requests_this_month >= settings.wnba_max_requests_per_month:
                logger.warning(
                    "WNBA monthly API budget reached: %s/%s",
                    requests_this_month,
                    settings.wnba_max_requests_per_month,
                )
                return False

        return True
    
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
        self._reset_monthly_usage_if_needed()
        self.monthly_usage[league] += 1
        
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
        self._reset_monthly_usage_if_needed()
        
        stats = {}
        for league in ['NBA', 'MLB', 'NHL', 'NFL', 'WNBA', 'MLS', 'IPL', 'MLC']:
            stats[league] = {
                'requests_last_minute': len(self.request_history[league]),
                'requests_today': self.daily_usage[league],
                'requests_this_month': self.monthly_usage[league],
                'max_per_minute': settings.get_max_requests_per_minute(league)
            }
            if league == 'NFL':
                stats[league]['max_per_day'] = settings.nfl_max_requests_per_day
                stats[league]['max_per_month'] = settings.nfl_max_requests_per_month
            if league == 'WNBA':
                stats[league]['max_per_month'] = settings.wnba_max_requests_per_month
                stats[league]['max_per_second'] = settings.wnba_max_requests_per_second
        
        return stats
    
    def get_monthly_usage(self, league: str) -> int:
        """
        Get monthly usage count for a league.
        
        Args:
            league: League identifier
            
        Returns:
            Number of requests made this month
        """
        self._reset_monthly_usage_if_needed()
        return self.monthly_usage[league]
    
    def can_make_monthly_request(self, league: str) -> bool:
        """
        Check if we can make a request without exceeding monthly limits.
        
        Args:
            league: League identifier
            
        Returns:
            True if request is allowed within monthly limit
        """
        self._reset_monthly_usage_if_needed()
        
        if league == 'NFL':
            monthly_limit = settings.nfl_max_requests_per_month
            current_monthly = self.monthly_usage[league]
            return current_monthly < monthly_limit
        if league == 'WNBA':
            monthly_limit = settings.wnba_max_requests_per_month
            current_monthly = self.monthly_usage[league]
            return current_monthly < monthly_limit
        
        return True  # No monthly limit for other leagues

    def _can_make_in_memory_budgeted_request(self, league: str) -> bool:
        self._reset_daily_usage_if_needed()
        self._reset_monthly_usage_if_needed()

        # In-memory hourly counter: count entries in request_history (last 60s
        # only) is too narrow; use daily_usage as a coarse signal and rely on
        # the DB-backed path for accurate hourly enforcement. The in-memory
        # path is just a fallback when DB is unavailable.
        if league == 'NFL':
            return self.daily_usage[league] < settings.nfl_max_requests_per_day
        if league == 'WNBA':
            if self.daily_usage[league] >= settings.wnba_max_requests_per_day:
                return False
            return self.monthly_usage[league] < settings.wnba_max_requests_per_month
        return True

    def _count_requests_since(self, db: Session, league: str, since: datetime) -> int:
        return db.query(ApiUsage).filter(
            ApiUsage.league == league,
            ApiUsage.timestamp >= since,
        ).count()
    
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
    
    def _reset_monthly_usage_if_needed(self):
        """Reset monthly usage if it's a new month."""
        today = datetime.now().date()
        first_of_month = today.replace(day=1)
        if first_of_month > self.last_monthly_reset:
            self.monthly_usage.clear()
            self.last_monthly_reset = first_of_month
            logger.info("Reset monthly API usage counters")


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
