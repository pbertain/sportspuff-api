# Sportspuff API

API interface for SportsPuff data, including schedules, scores and standings.

## Project Structure

```
sportspuff-api/
├── .github/workflows/         # GitHub Actions deployment workflows
├── ansible/                   # Ansible playbooks for deployment
│   ├── deploy.yml            # Main deployment playbook
│   ├── templates/            # Configuration templates
│   └── README.md             # Deployment documentation
└── sports-data-service/       # Application code
    ├── src/                  # Source code
    ├── scripts/              # Utility scripts
    ├── systemd/              # Systemd service files
    ├── docker-compose.yml    # Docker configuration
    └── README.md             # Application documentation
```

## Overview

This project provides a multi-league sports data collection service that fetches schedules and live scores from NBA, MLB, NHL, NFL, and WNBA APIs using adaptive polling.

## Deployment

### Automatic Deployment (GitHub Actions)

The project is configured for automatic deployment using GitHub Actions:

- **Production**: Pushes to `main` branch deploy to production (port 34180)
- **Development**: Pushes to `dev` branch deploy to development (port 34181)

### Setting Up GitHub Secrets

Before the first deployment, configure these secrets in your GitHub repository:

1. **Settings → Secrets and variables → Actions**

2. Add the following secrets:

   - **SSH_PRIVATE_KEY**: Content of your SSH private key file (`~/.ssh/keys/nirdclub__id_ed25519`)
   - **HOST_IP**: Your server IP address
   - **POSTGRES_PASSWORD**: Your PostgreSQL password (generate with `openssl rand -base64 32`)

### Manual Deployment

You can also deploy manually using Ansible:

```bash
# Production
ansible-playbook \
  -i "your-server-ip," \
  -u ansible \
  --private-key ~/.ssh/keys/nirdclub__id_ed25519 \
  -e "deployment_env=prod" \
  -e "api_port=34180" \
  -e "vault_postgres_password=your-password" \
  ansible/deploy.yml

# Development
ansible-playbook \
  -i "your-server-ip," \
  -u ansible \
  --private-key ~/.ssh/keys/nirdclub__id_ed25519 \
  -e "deployment_env=dev" \
  -e "api_port=34181" \
  -e "vault_postgres_password=your-password" \
  ansible/deploy.yml
```

## Documentation

- **Application Documentation**: See [sports-data-service/README.md](sports-data-service/README.md)
- **Deployment Documentation**: See [ansible/README.md](ansible/README.md)
- **API Setup**: See [sports-data-service/API_SETUP.md](sports-data-service/API_SETUP.md)
- **Deployment Guide**: See [sports-data-service/DEPLOYMENT.md](sports-data-service/DEPLOYMENT.md)

## Features

- Multi-League Support (NBA, MLB, NHL, NFL, WNBA)
- Adaptive Polling (1-minute for close games, 2-minute for regular)
- API Conservation (only polls when games are scheduled)
- PostgreSQL Storage with proper schema
- Docker Deployment with Docker Compose
- Systemd Integration for automated scheduling
- Rate Limiting and usage tracking

## Development

### Local Setup

```bash
cd sports-data-service
cp env.example .env
# Edit .env with your configuration
docker-compose up -d
```

See [sports-data-service/README.md](sports-data-service/README.md) for detailed setup and usage instructions.

## License

MIT License - see [LICENSE](sports-data-service/LICENSE) file for details.


