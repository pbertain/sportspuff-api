"""add nhl otl columns

Revision ID: 0001
Revises: 
Create Date: 2025-01-27 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add home_otl column
    op.add_column('games', sa.Column('home_otl', sa.Integer(), nullable=True))
    
    # Add visitor_otl column
    op.add_column('games', sa.Column('visitor_otl', sa.Integer(), nullable=True))


def downgrade() -> None:
    # Remove visitor_otl column
    op.drop_column('games', 'visitor_otl')
    
    # Remove home_otl column
    op.drop_column('games', 'home_otl')

