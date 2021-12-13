import structlog
import sqlalchemy
from datetime import datetime, timezone
from dateutil import tz
from sys import maxsize

from sqlalchemy import Integer, String, Boolean, BigInteger, \
    Column, ForeignKey, select, update
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import declarative_base, Session, relationship, backref
from sqlalchemy.types import TypeDecorator, DateTime

from typing import Dict, List, Optional, Any
from urllib.parse import parse_qs
from packaging.version import parse as parse_version

from . import VERSION


logger = structlog.getLogger(__name__)


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
    created_time = Column(TimestampWithTZ())
    modified_time = Column(TimestampWithTZ())
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

    @property
    def is_google_workspace_file(self) -> bool:
        return not self.is_directory and \
            (self.md5_checksum or '').startswith('application/vnd.google-apps')

    @property
    def can_download(self) -> bool:
        return not self.is_directory and not self.is_google_workspace_file

    @property
    def local_name(self) -> str:
        return self.id + \
            '_' + (self.head_revision_id or '') + \
            '_' + (self.md5_checksum or '')

    def as_dict(self, depth: int = maxsize,
                remove_gdfile_id: bool = False) -> Dict[str, Any]:
        d = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        d['can_download'] = self.can_download
        if remove_gdfile_id:
            del d['gdfile_id']
        if depth > 0 and self._children:
            d['children'] = [c.as_dict(depth - 1, remove_gdfile_id)
                             for c in self._children]
        if self.is_directory:
            for k in ['head_revision_id', 'size',
                      'md5_checksum', 'can_download']:
                d.pop(k, None)
        else:
            d.pop('is_root', None)
        return d


class FileToDelete(Base):

    __tablename__ = 'filetodelete'

    filetodelete_id = Column(Integer, primary_key=True)
    local_name: str = Column(String, nullable=False)

    id: str = Column(String, nullable=False)
    name = Column(String)
    modified_time = Column(TimestampWithTZ())
    mime_type = Column(String)
    size = Column(BigInteger)
    md5_checksum = Column(String)
    head_revision_id = Column(String)

    removedfromindexat: datetime = Column(
        TimestampWithTZ(), default=datetime.now())

    def as_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class DatabaseMetadata(Base):

    __tablename__ = '_db_metadata'

    _db_metadata_id = Column(Integer, primary_key=True)
    key = Column(String, unique=True)
    value = Column(String)

    def __repr__(self):
        return f'[{self.key} = {self.value}]'


class LocalStorageDatabase:

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
            log = logger.bind(new=False)
            if (version_row := result.first()):
                version = version_row[0].value
            else:
                version = VERSION
                log = logger.bind(new=True)
                row = DatabaseMetadata(key='version', value=version)
                session.add(row)
                session.commit()
            log.info('checking database version', version=version)

        def upgrade_1_0_0() -> None:
            pass

        upgraders = {
            '1.0.0': upgrade_1_0_0,
        }

        current_version = parse_version(version)
        for version, fn in upgraders.items():
            v = parse_version(version)
            if current_version < v:
                logger.info('upgrading database version',
                            old=current_version, new=v)
                with Session(self._engine) as session:
                    fn()
                    session.execute(
                        update(DatabaseMetadata).where(
                            DatabaseMetadata.key == 'version').
                        values(value=version))
                    session.commit()
                    current_version = v

    def get_tree(self, session: Session) -> Optional[LocalFile]:
        logger.info('requesting local tree')
        stmt = select(LocalFile).where(LocalFile.is_root)  # type: ignore
        result = session.execute(stmt)
        root = result.scalars().first()
        return root

    def get_file_by_id(self, session: Session, id: str) -> Optional[LocalFile]:
        log = logger.bind(id=id)
        log.info('requesting local file')
        stmt = select(LocalFile).where(LocalFile.id == id)  # type: ignore
        result = session.execute(stmt)
        f = result.scalars().first()
        log.info('requested local file', found=bool(f))
        return f

    def new_session(self) -> Session:
        return Session(self._engine)

    def get_files_to_delete(self, session: Session,
                            until: Optional[datetime] = None) \
            -> List[FileToDelete]:
        logger.info('requesting list of files to delete', until=until)
        stmt = select(FileToDelete)  # type: ignore
        if until:
            stmt = stmt.where(FileToDelete.removedfromindexat <= until)
        result = session.execute(stmt)
        return result.scalars().all()

    def get_file_to_delete(self, session: Session, id: str,
                           head_revision_id: Optional[str] = None) \
            -> Optional[FileToDelete]:
        logger.info('requesting a specific file marked for deletion', id=id)
        stmt = select(FileToDelete).where(  # type: ignore
            FileToDelete.id == id)
        if head_revision_id:
            stmt = stmt.where(
                FileToDelete.head_revision_id == head_revision_id)
        result = session.execute(stmt)
        return result.scalars().first()
