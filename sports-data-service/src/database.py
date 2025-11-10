"""
Database connection and session management.
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from typing import Generator
from urllib.parse import urlparse, quote, urlunparse

from config import settings

# Fix DATABASE_URL if password contains special characters that need URL encoding
def _fix_database_url(url: str) -> str:
    """
    Fix DATABASE_URL by properly URL-encoding the password if needed.
    
    Args:
        url: Database URL string
        
    Returns:
        Fixed database URL with properly encoded password
    """
    try:
        parsed = urlparse(url)
        # If password contains special characters, URL-encode it
        if parsed.password and ('/' in parsed.password or '=' in parsed.password or '@' in parsed.password or ':' in parsed.password):
            # Reconstruct URL with encoded password
            encoded_password = quote(parsed.password, safe='')
            netloc = f"{parsed.username}:{encoded_password}@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            fixed_url = urlunparse((
                parsed.scheme,
                netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment
            ))
            return fixed_url
        return url
    except Exception:
        # If parsing fails, return original URL
        return url

# Get database URL, fixing password encoding if needed
database_url = _fix_database_url(settings.database_url)

# Create database engine
engine = create_engine(
    database_url,
    pool_pre_ping=True,
    pool_recycle=300,
    echo=False  # Set to True for SQL query logging
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create base class for models
Base = declarative_base()


def get_db() -> Generator[Session, None, None]:
    """
    Dependency to get database session.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """
    Context manager for database sessions.
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def create_tables():
    """
    Create all tables in the database.
    """
    Base.metadata.create_all(bind=engine)


def drop_tables():
    """
    Drop all tables in the database.
    """
    Base.metadata.drop_all(bind=engine)
