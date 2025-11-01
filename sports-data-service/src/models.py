"""
SQLAlchemy models for the sports data service.
"""

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Date, Time, Text, JSON
from sqlalchemy.sql import func
from database import Base


class Game(Base):
    """Unified games table for all sports leagues."""
    
    __tablename__ = "games"
    
    # Primary key
    id = Column(Integer, primary_key=True, index=True)
    
    # League and game identification
    league = Column(String(10), nullable=False, index=True)
    game_id = Column(String(50), nullable=False, index=True)
    game_date = Column(Date, nullable=False, index=True)
    game_time = Column(DateTime(timezone=True), nullable=True)
    game_type = Column(String(20), nullable=False)  # preseason, regular, playoffs, allstar, nba_cup, etc.
    
    # Home team
    home_team = Column(String(100), nullable=False)
    home_team_abbrev = Column(String(10), nullable=False)
    home_team_id = Column(String(20), nullable=True)
    home_wins = Column(Integer, nullable=True)
    home_losses = Column(Integer, nullable=True)
    home_score_total = Column(Integer, nullable=True)
    
    # Visitor team
    visitor_team = Column(String(100), nullable=False)
    visitor_team_abbrev = Column(String(10), nullable=False)
    visitor_team_id = Column(String(20), nullable=True)
    visitor_wins = Column(Integer, nullable=True)
    visitor_losses = Column(Integer, nullable=True)
    visitor_score_total = Column(Integer, nullable=True)
    
    # Game state
    game_status = Column(String(20), nullable=False, index=True)  # scheduled, in_progress, final, postponed
    current_period = Column(String(20), nullable=True)
    time_remaining = Column(String(20), nullable=True)
    is_final = Column(Boolean, default=False, index=True)
    is_overtime = Column(Boolean, default=False)
    
    # Sport-specific scoring (JSON for flexibility)
    home_period_scores = Column(JSON, nullable=True)  # {"q1":25, "q2":30, ...} or {"1":2, "2":1, ...}
    visitor_period_scores = Column(JSON, nullable=True)
    
    # MLB specific
    home_hits = Column(Integer, nullable=True)
    home_runs = Column(Integer, nullable=True)
    home_errors = Column(Integer, nullable=True)
    visitor_hits = Column(Integer, nullable=True)
    visitor_runs = Column(Integer, nullable=True)
    visitor_errors = Column(Integer, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    def __repr__(self):
        return f"<Game(league={self.league}, game_id={self.game_id}, home={self.home_team} vs visitor={self.visitor_team})>"


class ApiUsage(Base):
    """API usage tracking table."""
    
    __tablename__ = "api_usage"
    
    id = Column(Integer, primary_key=True, index=True)
    league = Column(String(10), nullable=False, index=True)
    endpoint = Column(String(100), nullable=False)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    success = Column(Boolean, default=True, index=True)
    error_message = Column(Text, nullable=True)
    response_time_ms = Column(Integer, nullable=True)
    
    def __repr__(self):
        return f"<ApiUsage(league={self.league}, endpoint={self.endpoint}, success={self.success})>"
