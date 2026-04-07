"""
Template file used by Alembic for generated revisions. Keep the default template.
"""
##
## Note: Alembic will use its builtin template if this file is missing; this
## is provided to make the repository self-contained.
##
from alembic import op
import sqlalchemy as sa

${imports if imports else ""}

def upgrade():
    ${upgrades if upgrades else "pass"}


def downgrade():
    ${downgrades if downgrades else "pass"}
