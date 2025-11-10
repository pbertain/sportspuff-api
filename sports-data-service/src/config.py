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
    nba_api_timeout: int = Field(default=10)
    mlb_api_timeout: int = Field(default=10)
    nhl_api_timeout: int = Field(default=10)
    nfl_api_timeout: int = Field(default=10)
    wnba_api_timeout: int = Field(default=10)
    
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
    
    # Data retention
    max_season_size_mb: int = Field(default=10)
    cleanup_old_seasons: bool = Field(default=True)
    
    # Schedule update times (24-hour format)
    schedule_update_times: str = Field(default="06:00,18:00")
    
    # Live polling hours (24-hour format, comma-separated ranges)
    live_polling_hours: str = Field(default="12:00-02:00")
    
    # API Rate Limiting
    nba_max_requests_per_minute: int = Field(default=60)
    mlb_max_requests_per_minute: int = Field(default=30)
    nhl_max_requests_per_minute: int = Field(default=60)
    nfl_max_requests_per_minute: int = Field(default=30)
    wnba_max_requests_per_minute: int = Field(default=60)
    
    # Monthly API Limits (for Tank01/RapidAPI)
    nfl_max_requests_per_month: int = Field(default=10000, description="Monthly limit for NFL API (Tank01/RapidAPI)")
    
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
    
    @field_validator('proxy_enabled', mode='before')
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
        }
        return limits.get(league.upper(), 60)


# Global settings instance
settings = Settings()
