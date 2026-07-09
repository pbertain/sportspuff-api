"""
Configuration management for Sports Data Service.
"""

import os
from typing import List, Dict, Any
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Database
    database_url: str = Field(default="postgresql://sports_user:sports_password@localhost:5432/sports_data")
    postgres_password: str = Field(default="sports_password")
    
    # Logging
    log_level: str = Field(default="INFO")
    
    # API Configuration
    cricapi_key: str = Field(default="", description="CricAPI key for IPL/MLC cricket data")
    cricapi_max_requests_per_day: int = Field(default=2000, description="Daily request limit for the shared CricAPI key (IPL/MLC)")
    cricapi_max_requests_per_hour: int = Field(default=300, description="Hourly circuit-breaker for CricAPI; sized to catch runaway fan-outs without throttling normal cricket-day surges (~15% of daily)")
    cricapi_usage_reserve: int = Field(default=200, description="Headroom left below the CricAPI daily limit for other consumers of the shared key")
    cricapi_cache_dir: str = Field(default="", description="Directory for persisted CricAPI responses; defaults to <service>/cache/cricket")
    cricapi_live_refresh: bool = Field(default=True, description="Force-refresh in-progress/recently-ended cricket matches each build. Disable on dev to conserve the shared quota.")
    cricapi_cache_ttl: int = Field(default=900, description="TTL (s) for cached CricAPI series/series_info lookups. Raise on dev to refresh roughly hourly.")
    cricapi_season_cache_ttl: int = Field(default=300, description="TTL (s) for the whole-season feed response cache, bounding CricAPI spend under frequent calls.")

    # TheSportsDB (lifetime license; replacing several upstreams over time)
    thesportsdb_key: str = Field(default="", description="TheSportsDB API key (set via vault_thesportsdb_key in ansible)")
    thesportsdb_max_requests_per_hour: int = Field(default=600, description="Hourly circuit-breaker for TheSportsDB; conservative since their docs don't expose rate-limit headers")
    thesportsdb_cache_dir: str = Field(default="", description="Directory for persisted TheSportsDB responses; defaults to <service>/cache/thesportsdb")
    thesportsdb_season_cache_ttl: int = Field(default=300, description="TTL (s) for the bulk-season events response in memory; disk cache is served indefinitely as a fallback")
    nba_provider: str = Field(default="thesportsdb", description="NBA data source: thesportsdb | nba_api")
    cricket_provider: str = Field(default="thesportsdb", description="Cricket (IPL/MLC) data source: thesportsdb | cricapi")
    cricket_live_enrichment: str = Field(default="cricapi", description="Augment in-progress cricket matches with rich CricAPI detail (overs, wickets, per-inning). 'cricapi' uses match_info on live games only; 'none' disables.")
    cycling_provider: str = Field(default="thesportsdb", description="Cycling data source: thesportsdb | file")
    cycling_data_dir: str = Field(default="", description="Directory containing cycling CSV inputs (cycling_stages.csv, cycling_gc.csv, cycling_team_classification.csv, cycling_jerseys.csv)")
    tour_de_france_data_dir: str = Field(default="", description="Directory containing Tour de France bundle files from letour-scraper.")
    la_vuelta_data_dir: str = Field(default="", description="Directory containing La Vuelta bundle files from lavuelta-scraper.")
    nba_api_timeout: int = Field(default=10)
    mlb_api_timeout: int = Field(default=10)
    nhl_api_timeout: int = Field(default=10)
    nfl_api_timeout: int = Field(default=10)
    wnba_api_timeout: int = Field(default=10)
    mls_api_timeout: int = Field(default=10)
    ipl_api_timeout: int = Field(default=10)
    mlc_api_timeout: int = Field(default=10)
    
    # Polling Configuration
    default_poll_interval: int = Field(default=120)  # 2 minutes
    close_game_poll_interval: int = Field(default=60)  # 1 minute
    scheduled_game_poll_interval: int = Field(default=300)  # 5 minutes
    
    # Close game thresholds by league
    nba_close_game_threshold: int = Field(default=10)
    nfl_close_game_threshold: int = Field(default=10)
    nhl_close_game_threshold: int = Field(default=2)
    mlb_close_game_threshold: int = Field(default=3)
    wnba_close_game_threshold: int = Field(default=10)
    mls_close_game_threshold: int = Field(default=2)
    ipl_close_game_threshold: int = Field(default=20)
    mlc_close_game_threshold: int = Field(default=20)
    
    # Data retention
    max_season_size_mb: int = Field(default=10)
    cleanup_old_seasons: bool = Field(default=True)
    
    # Schedule update times (24-hour format)
    schedule_update_times: str = Field(default="06:00,18:00")
    
    # Live polling hours (24-hour format, comma-separated ranges)
    live_polling_hours: str = Field(default="10:00-23:30")
    
    # API Rate Limiting
    nba_max_requests_per_minute: int = Field(default=60)
    mlb_max_requests_per_minute: int = Field(default=30)
    nhl_max_requests_per_minute: int = Field(default=60)
    nfl_max_requests_per_minute: int = Field(default=30)
    wnba_max_requests_per_minute: int = Field(default=60)
    mls_max_requests_per_minute: int = Field(default=60)
    ipl_max_requests_per_minute: int = Field(default=60)
    mlc_max_requests_per_minute: int = Field(default=60)
    
    # Paid API limits (for Tank01/RapidAPI)
    nfl_max_requests_per_day: int = Field(default=1000, description="Daily included request limit for NFL API (Tank01/RapidAPI)")
    nfl_max_requests_per_hour: int = Field(default=150, description="Hourly circuit-breaker for NFL Tank01 (~15% of daily)")
    nfl_max_requests_per_month: int = Field(default=10000, description="Monthly limit for NFL API (Tank01/RapidAPI)")
    wnba_max_requests_per_month: int = Field(default=14000, description="Monthly included request limit for WNBA API (RapidAPI)")
    wnba_max_requests_per_day: int = Field(default=460, description="Daily soft cap for WNBA API to keep monthly under quota (~14000/30)")
    wnba_max_requests_per_hour: int = Field(default=80, description="Hourly circuit-breaker for WNBA RapidAPI (~17% of daily)")
    wnba_max_requests_per_second: int = Field(default=10, description="Per-second rate limit for WNBA API (RapidAPI)")
    
    # API Server
    api_host: str = Field(default="127.0.0.1")
    api_port: int = Field(default=34180)
    
    # Proxy Configuration (for Decodo or other proxy services)
    proxy_enabled: bool = Field(default=False, description="Enable proxy for NBA API requests")
    proxy_host: str = Field(default="dc.decodo.com")
    proxy_username: str = Field(default="")
    proxy_password: str = Field(default="")
    proxy_port_start: int = Field(default=10001)
    proxy_port_end: int = Field(default=10010)
    
    @field_validator('proxy_enabled', 'cricapi_live_refresh', mode='before')
    @classmethod
    def parse_bool(cls, v):
        """Parse boolean from string or bool."""
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.lower() in ('true', '1', 'yes', 'on')
        return bool(v)
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"
    
    @property
    def schedule_update_times_list(self) -> List[str]:
        """Parse schedule update times into a list."""
        return [time.strip() for time in self.schedule_update_times.split(",")]
    
    @property
    def live_polling_hours_list(self) -> List[str]:
        """Parse live polling hours into a list."""
        return [hours.strip() for hours in self.live_polling_hours.split(",")]
    
    def get_close_game_threshold(self, league: str) -> int:
        """Get close game threshold for a specific league."""
        thresholds = {
            "NBA": self.nba_close_game_threshold,
            "NFL": self.nfl_close_game_threshold,
            "NHL": self.nhl_close_game_threshold,
            "MLB": self.mlb_close_game_threshold,
            "WNBA": self.wnba_close_game_threshold,
            "MLS": self.mls_close_game_threshold,
            "IPL": self.ipl_close_game_threshold,
            "MLC": self.mlc_close_game_threshold,
        }
        return thresholds.get(league.upper(), self.nba_close_game_threshold)
    
    def get_api_timeout(self, league: str) -> int:
        """Get API timeout for a specific league."""
        timeouts = {
            "NBA": self.nba_api_timeout,
            "MLB": self.mlb_api_timeout,
            "NHL": self.nhl_api_timeout,
            "NFL": self.nfl_api_timeout,
            "WNBA": self.wnba_api_timeout,
            "MLS": self.mls_api_timeout,
            "IPL": self.ipl_api_timeout,
            "MLC": self.mlc_api_timeout,
        }
        return timeouts.get(league.upper(), 10)
    
    def get_max_requests_per_minute(self, league: str) -> int:
        """Get max requests per minute for a specific league."""
        limits = {
            "NBA": self.nba_max_requests_per_minute,
            "MLB": self.mlb_max_requests_per_minute,
            "NHL": self.nhl_max_requests_per_minute,
            "NFL": self.nfl_max_requests_per_minute,
            "WNBA": self.wnba_max_requests_per_minute,
            "MLS": self.mls_max_requests_per_minute,
            "IPL": self.ipl_max_requests_per_minute,
            "MLC": self.mlc_max_requests_per_minute,
        }
        return limits.get(league.upper(), 60)


# Global settings instance
settings = Settings()
