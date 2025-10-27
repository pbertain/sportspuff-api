# SportsPuff API Ansible Deployment

This directory contains the Ansible playbooks for deploying SportsPuff API to the production and development environments.

## Overview

- **Production (main branch)**: Port 34180
- **Development (dev branch)**: Port 34181
- **Deployment directory**: `/opt/sportspuff-api`

## Files

- `deploy.yml` - Main deployment playbook
- `hosts` - Inventory file (manages server hosts)
- `templates/env.j2` - Environment configuration template
- `templates/docker-compose.override.yml.j2` - Docker Compose override template
- `ansible.cfg` - Ansible configuration

## GitHub Secrets Required

The following secrets must be configured in GitHub repository settings:

1. **SSH_PRIVATE_KEY** - Your SSH private key (content of `~/.ssh/keys/nirdclub__id_ed25519`)
2. **POSTGRES_PASSWORD** - PostgreSQL password

**Note**: Host configuration is managed in `ansible/hosts` file. Update the hostname there if needed.

## Setup

### 1. Configure Hosts

Edit `ansible/hosts` to add or modify server hosts:

```ini
[prod]
host74.nird.club

[dev]
host74.nird.club
```

### 2. Add GitHub Secrets

Go to your repository Settings → Secrets and variables → Actions, and add:

1. **SSH_PRIVATE_KEY**
   - Name: `SSH_PRIVATE_KEY`
   - Value: Contents of `~/.ssh/keys/nirdclub__id_ed25519` file

2. **POSTGRES_PASSWORD**
   - Name: `POSTGRES_PASSWORD`
   - Value: Your PostgreSQL password (e.g., generate with `openssl rand -base64 32`)

### 3. Server Preparation

Ensure the following are installed on your server:

```bash
# Install Docker and Docker Compose
sudo apt-get update
sudo apt-get install -y docker.io docker-compose

# Add ansible user to docker group (if using docker)
sudo usermod -aG docker ansible

# Create deployment directory
sudo mkdir -p /opt/sportspuff-api
sudo chown ansible:ansible /opt/sportspuff-api
```

## Deployment Process

### Automatic (via GitHub Actions)

Deployments are triggered automatically when you push to:
- `main` branch → Production environment (port 34180)
- `dev` branch → Development environment (port 34181)

### Manual Deployment

You can also run the playbook manually from your local machine:

```bash
cd ansible

# Production
ansible-playbook \
  --limit prod \
  -e "deployment_env=prod" \
  -e "api_port=34180" \
  -e "vault_postgres_password=your-password" \
  deploy.yml

# Development
ansible-playbook \
  --limit dev \
  -e "deployment_env=dev" \
  -e "api_port=34181" \
  -e "vault_postgres_password=your-password" \
  deploy.yml
```

## What the Deployment Does

1. Creates deployment directory structure at `/opt/sportspuff-api`
2. Backs up current deployment (if exists) to `/opt/sportspuff-api-backups/`
3. Copies application files from repository
4. Creates environment-specific configuration
5. Stops existing containers
6. Builds and starts new containers with docker-compose
7. Waits for services to be healthy
8. Initializes database (first time only)

## Troubleshooting

### View logs on server

```bash
ssh ansible@your-server
cd /opt/sportspuff-api/sports-data-service
docker-compose logs -f
```

### Check service status

```bash
cd /opt/sportspuff-api/sports-data-service
docker-compose ps
docker-compose exec sports-service ps aux
```

### Manual database initialization

```bash
cd /opt/sportspuff-api/sports-data-service
docker-compose exec sports-service python scripts/update_schedules.py
```

### Check API health

```bash
# Production
curl http://localhost:34180/health

# Development
curl http://localhost:34181/health
```

## Backup and Rollback

Backups are automatically created in `/opt/sportspuff-api-backups/` before each deployment with timestamp.

To rollback:

```bash
# SSH to server
ssh ansible@your-server

# Stop current deployment
cd /opt/sportspuff-api/sports-data-service
docker-compose down

# Restore from backup
rm -rf /opt/sportspuff-api/sports-data-service
cp -r /opt/sportspuff-api-backups/backup-TIMESTAMP /opt/sportspuff-api/sports-data-service

# Restart
cd /opt/sportspuff-api/sports-data-service
docker-compose up -d
```

## File Structure on Server

After deployment, your server will have:

```
/opt/sportspuff-api/
├── sports-data-service/          # Application files
│   ├── src/                      # Source code
│   ├── scripts/                  # Python scripts
│   ├── systemd/                  # Systemd service files
│   ├── docker-compose.yml       # Docker configuration
│   ├── docker-compose.override.yml  # Environment override
│   └── .env                      # Environment variables
└── backups/                      # Deployment backups
    └── backup-{timestamp}/
```

## Notes

- The playbook uses `docker-compose` for container management
- PostgreSQL data is persisted in Docker volumes
- Each environment (dev/prod) has its own port and database instance
- The deployment is idempotent and can be run multiple times safely
- All containers run with the `ansible` user
