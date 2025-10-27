# Sports Data Service

A comprehensive, multi-league sports data collection service that fetches schedules and live scores from NBA, MLB, NHL, NFL, and WNBA APIs. The service uses adaptive polling to minimize API calls and stores data in PostgreSQL with proper schema for each sport's unique scoring system.

## Features

- **Multi-League Support**: NBA, MLB, NHL, NFL, WNBA
- **Adaptive Polling**: 1-minute intervals for close games, 2-minute for others, no polling when all games are final
- **API Conservation**: Only polls when games are scheduled/in progress
- **PostgreSQL Storage**: Persistent data with proper schema for each sport
- **Docker Deployment**: Easy deployment with Docker Compose
- **Systemd Integration**: Automated scheduling with systemd timers
- **Rate Limiting**: Built-in API rate limiting and usage tracking
- **Data Retention**: Configurable cleanup of old season data

## Architecture

### Components

1. **Data Collectors** - League-specific collectors that handle API interactions
2. **PostgreSQL Database** - Persistent storage with league-agnostic schema
3. **Schedule Updater** - Systemd timer running twice daily (6 AM, 6 PM)
4. **Live Score Poller** - Adaptive polling service during game times
5. **Docker Setup** - Docker Compose with PostgreSQL + Python service
6. **Configuration** - Environment-based config for all settings

### Database Schema

The service uses a unified `games` table that accommodates all sports:

```sql
CREATE TABLE games (
    id SERIAL PRIMARY KEY,
    league VARCHAR(10) NOT NULL,
    game_id VARCHAR(50) NOT NULL,
    game_date DATE NOT NULL,
    game_time TIMESTAMP WITH TIME ZONE,
    game_type VARCHAR(20) NOT NULL,
    
    -- Home team
    home_team VARCHAR(100) NOT NULL,
    home_team_abbrev VARCHAR(10) NOT NULL,
    home_team_id VARCHAR(20),
    home_wins INTEGER,
    home_losses INTEGER,
    home_score_total INTEGER,
    
    -- Visitor team
    visitor_team VARCHAR(100) NOT NULL,
    visitor_team_abbrev VARCHAR(10) NOT NULL,
    visitor_team_id VARCHAR(20),
    visitor_wins INTEGER,
    visitor_losses INTEGER,
    visitor_score_total INTEGER,
    
    -- Game state
    game_status VARCHAR(20) NOT NULL,
    current_period VARCHAR(20),
    time_remaining VARCHAR(20),
    is_final BOOLEAN DEFAULT FALSE,
    is_overtime BOOLEAN DEFAULT FALSE,
    
    -- Sport-specific scoring (JSON for flexibility)
    home_period_scores JSONB,
    visitor_period_scores JSONB,
    
    -- MLB specific fields
    home_hits INTEGER,
    home_runs INTEGER,
    home_errors INTEGER,
    visitor_hits INTEGER,
    visitor_runs INTEGER,
    visitor_errors INTEGER,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(league, game_id)
);
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)
- PostgreSQL (if not using Docker)

### Installation

1. **Clone and setup**:
   ```bash
   git clone <repository-url>
   cd sports-data-service
   cp env.example .env
   # Edit .env with your configuration
   ```

2. **Start with Docker Compose**:
   ```bash
   docker-compose up -d
   ```

3. **Initialize database**:
   ```bash
   docker-compose exec sports-service python scripts/update_schedules.py
   ```

4. **Start live polling**:
   ```bash
   docker-compose exec sports-service python scripts/poll_live_scores.py --once
   ```

### Configuration

Copy `env.example` to `.env` and configure:

```bash
# Database
DATABASE_URL=postgresql://sports_user:sports_password@localhost:5432/sports_data
POSTGRES_PASSWORD=sports_password

# Polling Configuration
DEFAULT_POLL_INTERVAL=120  # 2 minutes
CLOSE_GAME_POLL_INTERVAL=60  # 1 minute
SCHEDULED_GAME_POLL_INTERVAL=300  # 5 minutes

# Close game thresholds by league
NBA_CLOSE_GAME_THRESHOLD=10
NFL_CLOSE_GAME_THRESHOLD=10
NHL_CLOSE_GAME_THRESHOLD=2
MLB_CLOSE_GAME_THRESHOLD=3
WNBA_CLOSE_GAME_THRESHOLD=10

# Schedule update times (24-hour format)
SCHEDULE_UPDATE_TIMES=06:00,18:00

# Live polling hours
LIVE_POLLING_HOURS=12:00-02:00

# API Rate Limiting
NBA_MAX_REQUESTS_PER_MINUTE=60
MLB_MAX_REQUESTS_PER_MINUTE=30
NHL_MAX_REQUESTS_PER_MINUTE=60
NFL_MAX_REQUESTS_PER_MINUTE=30
WNBA_MAX_REQUESTS_PER_MINUTE=60
```

## Usage

### Manual Operations

**Update schedules**:
```bash
# Update all leagues
python scripts/update_schedules.py

# Update specific league
python scripts/update_schedules.py --league NBA

# Update for specific date
python scripts/update_schedules.py --date 2024-12-15

# Show statistics
python scripts/update_schedules.py --stats
```

**Poll live scores**:
```bash
# Poll once and exit
python scripts/poll_live_scores.py --once

# Poll specific league
python scripts/poll_live_scores.py --league NBA --once

# Show polling status
python scripts/poll_live_scores.py --status

# Force update all active games
python scripts/poll_live_scores.py --force
```

### Systemd Integration

**Install services**:
```bash
sudo cp systemd/*.service /etc/systemd/system/
sudo cp systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

**Enable and start**:
```bash
# Schedule updates (twice daily)
sudo systemctl enable sports-schedule-update.timer
sudo systemctl start sports-schedule-update.timer

# Live polling (during game hours)
sudo systemctl enable sports-live-poller.service
sudo systemctl start sports-live-poller.service
```

**Check status**:
```bash
sudo systemctl status sports-schedule-update.timer
sudo systemctl status sports-live-poller.service
```

## API Data Sources

- **NBA**: Official NBA Stats API (via `nba_api` library)
- **MLB**: Official MLB Stats API (via `mlb-statsapi` library)
- **NHL**: NHL Web API (public, no key required)
- **NFL**: Tank01 NFL API (RapidAPI)
- **WNBA**: Tank01 WNBA API (RapidAPI)

## Adaptive Polling Logic

The service uses intelligent polling to minimize API calls:

1. **Check schedule first**: Only poll if games are scheduled today
2. **Adaptive intervals**:
   - Close games (≤10 points/goals): 1 minute
   - Regular games: 2 minutes
   - Scheduled games: 5 minutes
   - All games final: Stop polling
3. **Time-based polling**: Only poll during configured hours (default: 12 PM - 2 AM ET)

## Data Retention

- **Default**: Keep data if <10MB per league per season
- **Cleanup**: Configurable cleanup of old season data
- **Manual cleanup**: `python scripts/update_schedules.py --cleanup`

## Development

### Project Structure

```
sports-data-service/
├── docker-compose.yml          # Docker setup
├── Dockerfile                  # Python service container
├── requirements.txt            # Python dependencies
├── env.example                 # Configuration template
├── init.sql                    # Database initialization
├── alembic.ini                 # Database migrations
├── alembic/                    # Migration scripts
├── src/                        # Source code
│   ├── config.py              # Configuration management
│   ├── database.py            # Database connection
│   ├── models.py              # SQLAlchemy models
│   ├── collectors/            # League-specific collectors
│   ├── services/              # Core services
│   └── utils/                 # Utilities
├── scripts/                   # Executable scripts
├── systemd/                   # Systemd service files
└── README.md                  # This file
```

### Adding New Leagues

1. **Create collector**: Extend `BaseCollector` in `src/collectors/`
2. **Add to services**: Update `ScheduleUpdater` and `LivePoller`
3. **Update configuration**: Add league-specific settings
4. **Test**: Add tests and verify API integration

### Database Migrations

```bash
# Create migration
alembic revision --autogenerate -m "Description"

# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1
```

## Monitoring

### Logs

- **Docker**: `docker-compose logs -f sports-service`
- **Systemd**: `journalctl -u sports-schedule-update.service -f`

### Health Checks

- **Database**: PostgreSQL health check in Docker Compose
- **Service**: HTTP health endpoint (if implemented)
- **API Usage**: Tracked in `api_usage` table

### Metrics

- Games stored per league
- API requests per minute
- Polling frequency
- Error rates

## Troubleshooting

### Common Issues

1. **API Rate Limits**: Check `api_usage` table, adjust polling intervals
2. **Database Connection**: Verify PostgreSQL is running and accessible
3. **Missing Games**: Check API availability and error logs
4. **High CPU Usage**: Adjust polling intervals or disable unnecessary leagues

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
python scripts/update_schedules.py --stats
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Create an issue on GitHub
- Check the logs for error details
- Verify API keys and rate limits
- Test with `--once` flags first
