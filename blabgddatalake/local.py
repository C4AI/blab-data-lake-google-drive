import logging
import sqlalchemy
from datetime import datetime, timezone
from dateutil import tz

from sqlalchemy import Integer, String, Boolean, BigInteger, \
    Column, ForeignKey, select, update
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import declarative_base, Session, relationship, backref
from sqlalchemy.types import TypeDecorator, DateTime

from typing import Dict, List, Optional, Any
from urllib.parse import parse_qs
from packaging.version import parse as parse_version

from . import VERSION


logger = logging.getLogger(__package__)


Base = declarative_base()


class TimestampWithTZ(TypeDecorator):
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: Any, _dialect: Dialect) -> datetime:
        if value.tzinfo is None:
            value = value.astimezone(tz.tzlocal())
        return value.astimezone(timezone.utc)

    def process_result_value(self, value: Any, _dialect: Dialect) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class LocalFile(Base):

    __tablename__ = 'gdfile'

    gdfile_id = Column(Integer, primary_key=True)
    id: str = Column(String, unique=True, nullable=False)
    name = Column(String)
    created_time = Column(TimestampWithTZ(timezone=True))
    modified_time = Column(TimestampWithTZ(timezone=True))
    modified_by = Column(String)
    web_url = Column(String)
    is_root = Column(Boolean, default=False)
    mime_type = Column(String)
    size = Column(BigInteger)
    md5_checksum = Column(String)
    head_revision_id = Column(String)

    parent_id = Column(String, ForeignKey(id))
    parent: 'LocalFile' = relationship(
        'LocalFile',
        backref=backref('_children'), remote_side=[id]
    )

    _children: Optional[List['LocalFile']]

    @property
    def is_directory(self) -> bool:
        return self.mime_type == 'application/vnd.google-apps.folder'

    @property
    def children(self) -> Optional[List['LocalFile']]:
        return (self._children or []) if self.is_directory else None

    def __repr__(self) -> str:
        return '(name={}, gdid={}, size={}, md5={})'.format(
            self.name, self.gdfile_id, self.size, self.md5_checksum)

    def print_tree(self, pfx: Optional[List[bool]] = None) -> None:
        if pfx is None:
            pfx = []
        for i, p in enumerate(pfx[:-1]):
            print(' ┃ ' if p else '   ', end=' ')
        if pfx:
            print(' ┠─' if pfx[-1] else ' ┖─', end=' ')
        print(self.name)
        for child in (self.children or [])[:-1]:
            child.print_tree(pfx + [True])
        if self.children:
            self.children[-1].print_tree(pfx + [False])

    def flatten(self) -> Dict[str, 'LocalFile']:
        d: Dict[str, 'LocalFile'] = {self.id: self}
        for c in self.children or []:
            d.update(c.flatten() if isinstance(c, LocalFile)
                     else {c.id: c})
        return d


class DatabaseMetadata(Base):

    __tablename__ = '_db_metadata'

    _db_metadata_id = Column(Integer, primary_key=True)
    key = Column(String, unique=True)
    value = Column(String)

    def __repr__(self):
        return f'[{self.key} = {self.value}]'


class LocalStorageDatabse:

    def __init__(self, db_config: Dict[str, str]):
        self.db_config: Dict[str, str] = db_config
        self._engine = self.__create_engine()
        Base.metadata.create_all(self._engine)
        self.upgrade()

    def __create_engine(self) -> sqlalchemy.engine.base.Engine:
        cfg = self.db_config
        driver = cfg.get('Driver', '')
        url = sqlalchemy.engine.URL.create(
            cfg['Dialect'] + ('+' if driver else '') + driver,
            username=cfg.get('Username', None),
            password=cfg.get('Password', None),
            host=cfg.get('Host', None),
            port=int(p) if (p := cfg.get('Port', None)) else None,
            database=cfg.get('Database', None),
            query=parse_qs(cfg.get('Query', ''))
        )
        return sqlalchemy.create_engine(url)

    def upgrade(self) -> None:
        with Session(self._engine) as session:
            stmt = select(DatabaseMetadata).where(  # type: ignore
                DatabaseMetadata.key == 'version')
            result = session.execute(stmt)
            if (version_row := result.first()):
                version = version_row[0].value
                logger.info(f'Database version: {version}')
            else:
                version = VERSION
                row = DatabaseMetadata(key='version', value=version)
                session.add(row)
                session.commit()
                logger.info(f'Database version (new): {version}')

        def upgrade_1_0_0() -> None:
            pass

        upgraders = {
            '1.0.0': upgrade_1_0_0,
        }

        current_version = parse_version(version)
        for version, fn in upgraders.items():
            v = parse_version(version)
            if current_version < v:
                logger.info(f'Upgrading database to version {version}')
                with Session(self._engine) as session:
                    fn()
                    session.execute(
                        update(DatabaseMetadata).where(
                            DatabaseMetadata.key == 'version').
                        values(value=version))
                    session.commit()
                    current_version = v

    def get_tree(self, session: Session) -> Optional[LocalFile]:
        stmt = select(LocalFile).where(LocalFile.is_root)  # type: ignore
        result = session.execute(stmt)
        root = result.scalars().first()
        return root

    def new_session(self) -> Session:
        return Session(self._engine)
