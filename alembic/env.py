"""Alembic environment for autogenerate using project models.

Reads DB URL from `config.settings.get_settings().postgres_dsn` and sets
`target_metadata` to `db.models.Base.metadata` so `alembic revision --autogenerate`
works.
"""

from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context
from config.settings import get_settings
from db.models import Base as ModelsBase

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Load project settings
cfg = get_settings()

# Set SQLAlchemy URL from project settings
config.set_main_option("sqlalchemy.url", cfg.postgres_dsn)

target_metadata = ModelsBase.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata, compare_type=True
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
