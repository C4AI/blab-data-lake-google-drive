"""Deals with local copies of Google Drive files and their metadata."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from dateutil import tz
from sqlalchemy import (TypeDecorator, DateTime, Unicode, Column, Integer,
                        String)
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import declarative_base
from structlog import getLogger

_logger = getLogger(__name__)

Base = declarative_base()


class _TimestampWithTZ(TypeDecorator):
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: Any, _dialect: Dialect) \
            -> datetime | None:
        if not isinstance(value, datetime):
            return None
        if value.tzinfo is None:
            value = value.astimezone(tz.tzlocal())
        return value.astimezone(timezone.utc)

    def process_result_value(self, value: Any, _dialect: Dialect) \
            -> datetime | None:
        if not isinstance(value, datetime):
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class _CommaSeparatedValues(TypeDecorator):
    impl = Unicode

    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Dialect) -> str:
        return ','.join(value)

    def process_result_value(self, value: Any, dialect: Dialect) -> list[str]:
        return value.split(',') if isinstance(value, str) and value else []


class DatabaseMetadata(Base):
    """Represents metadata such as the program version."""

    __tablename__ = '_db_metadata'

    _db_metadata_id: int = Column(Integer, primary_key=True)
    """Internal id used by the database engine."""

    key: str = Column(String, unique=True)
    """Metadata key"""

    value: str = Column(String)
    """Metadata value corresponding to key"""

    def __repr__(self) -> str:
        return f'[{self.key} = {self.value}]'
