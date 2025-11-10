"""Add teams table for tracking team IDs across leagues

Revision ID: 0002_add_teams_table
Revises: 0001_add_nhl_otl_columns
Create Date: 2025-11-10 05:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0002_add_teams_table'
down_revision = '0001_add_nhl_otl_columns'
branch_labels = None
depends_on = None


def upgrade():
    # Create teams table
    op.create_table(
        'teams',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('league', sa.String(10), nullable=False),
        sa.Column('team_name', sa.String(100), nullable=False),
        sa.Column('team_abbrev', sa.String(10), nullable=False),
        sa.Column('api_team_id', sa.String(20), nullable=True),  # NHL/NBA/etc API team ID
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('league', 'api_team_id', name='uq_league_api_team_id'),
        sa.UniqueConstraint('league', 'team_abbrev', name='uq_league_abbrev')
    )
    
    # Create indexes
    op.create_index('idx_teams_league', 'teams', ['league'])
    op.create_index('idx_teams_api_team_id', 'teams', ['api_team_id'])
    op.create_index('idx_teams_abbrev', 'teams', ['team_abbrev'])


def downgrade():
    op.drop_index('idx_teams_abbrev', table_name='teams')
    op.drop_index('idx_teams_api_team_id', table_name='teams')
    op.drop_index('idx_teams_league', table_name='teams')
    op.drop_table('teams')

