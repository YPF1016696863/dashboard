"""empty message

Revision ID: 745c049592b9
Revises: e5c7a4e2df4d
Create Date: 2019-11-12 23:03:19.196443

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '745c049592b9'
down_revision = 'e5c7a4e2df4d'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('visualizations', sa.Column('user_id', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'visualizations', 'users', ['user_id'], ['id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'visualizations', type_='foreignkey')
    op.drop_column('visualizations', 'user_id')
    # ### end Alembic commands ###
