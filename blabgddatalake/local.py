"""Deals with local copies of Google Drive files and their metadata."""

from __future__ import annotations

from datetime import datetime, timezone
from dateutil import tz
from overrides import overrides
from packaging.version import parse as parse_version
from sqlalchemy import Integer, String, Boolean, BigInteger, create_engine, \
    Column, ForeignKey, select, update, UniqueConstraint
from sqlalchemy.engine import Dialect
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine import URL as sqlalchemy_url
from sqlalchemy.orm import declarative_base, Session, relationship, backref
from sqlalchemy.types import TypeDecorator, DateTime
from structlog import getLogger
from sys import maxsize
from typing import Any
from urllib.parse import parse_qs

from . import __version__ as VERSION


logger = getLogger(__name__)


Base = declarative_base()


class _TimestampWithTZ(TypeDecorator):
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: Any, _dialect: Dialect) \
            -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.astimezone(tz.tzlocal())
        return value.astimezone(timezone.utc)

    def process_result_value(self, value: Any, _dialect: Dialect) \
            -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class LocalFile(Base):
    """Represents a file or directory from Google Drive.

    Instances can be regular files, Google Workspace files or directories.
    """

    __tablename__ = 'gdfile'

    gdfile_id: int = Column(Integer, primary_key=True)
    """File id (used internally by the database engine)"""

    id: str = Column(String, unique=True, nullable=False)
    """File id (generated by Google Drive)"""

    name: str = Column(String)
    """File name (without directory)"""

    created_time: datetime = Column(_TimestampWithTZ())
    """Creation timestamp"""

    web_url: str = Column(String)
    """URL to access the file on a browser"""

    icon_url: str = Column(String)
    """URL of the file icon (does not require authentication)"""

    parent_id: str | None = Column(String, ForeignKey(id))
    """Id of the parent directory"""

    parent: LocalDirectory = relationship(
        'LocalDirectory',
        backref=backref('_children'), remote_side=[id]
    )

    modified_time: datetime = Column(_TimestampWithTZ())
    """Last modification timestamp"""

    modified_by = Column(String)
    """Name of the user who made the last change"""

    mime_type: str | None = Column(String)
    """MIME type"""

    obsolete_since: datetime = Column(_TimestampWithTZ(), nullable=True)

    type: str = Column(String)

    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'generic file'
    }

    def __repr__(self) -> str:
        return f'(name={self.name}, gdid={self.gdfile_id})'

    def print_tree(self, _pfx: list[bool] | None = None) -> None:
        """Print the tree file names to standard output (for debugging)."""
        if _pfx is None:
            _pfx = []
        for i, p in enumerate(_pfx[:-1]):
            print(' ┃ ' if p else '   ', end=' ')
        if _pfx:
            print(' ┠─' if _pfx[-1] else ' ┖─', end=' ')
        print(self.name)

    @property
    def virtual_path(self) -> list[str]:
        """Return a virtual path to this file on Google Drive.

        Returns:
            a list of directory names starting from the root,
            where each directory is a child of its predecessor,
            ended by the name of this file itself
        """
        p = []
        if self.parent and not self.parent.is_root:
            p = self.parent.virtual_path
        return p + [self.name or '']

    def as_dict(self, depth: int = maxsize,
                remove_gdfile_id: bool = False) -> dict[str, Any]:
        """Prepare the object to be serialised by converting it to a dict.

        Args:
            depth: maximum depth
            remove_gdfile_id: remove internal id

        Returns:
            a dictionary with the object data
        """
        d = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        d['virtual_path'] = self.virtual_path
        if remove_gdfile_id:
            del d['gdfile_id']
        return d


class LocalRegularFile(LocalFile):
    """Represents a regular file from Google Drive.

    Google Workspace files are not included.
    """

    @property
    def md5_checksum(self) -> str:
        """File hash.

        Returns:
            MD5 sum of the current revision of the file
        """
        return self.head_revision.md5_checksum

    @property
    def local_name(self) -> str:
        """Local file name (without path).

        Returns:
            Local file name
        """
        return self.id + \
            '_' + (self.head_revision_id or '') + \
            '_' + (self.md5_checksum or '')

    @property
    def size(self) -> int:
        """Size in bytes.

        Returns:
            Size of the current revision of the file
        """
        return self.head_revision.size

    head_revision_id: str | None = Column(String)
    """Current revision id (generated by Google Drive)"""

    head_revision: LocalFileRevision = relationship(
        'LocalFileRevision',
        primaryjoin='''and_(
            LocalRegularFile.id == LocalFileRevision.file_id,
            LocalRegularFile.head_revision_id == LocalFileRevision.revision_id
        )''', uselist=False
    )

    _revisions: list[LocalFileRevision]

    @property
    def revisions(self) -> list[LocalFileRevision]:
        """Return currently existing file revisions.

        Returns:
            The latest revision and past revisions that have been
            marked for deletion but have not been deleted yet
        """
        return self._revisions

    __mapper_args__ = {
        'polymorphic_identity': 'regular file'
    }


class LocalGoogleWorkspaceFile(LocalFile):
    """Represents a Google Workspace file from Google Drive."""

    __mapper_args__ = {
        'polymorphic_identity': 'Google Workspace file'
    }


class LocalDirectory(LocalFile):
    """Represents a Google Drive directory."""

    is_root: bool | None = Column(Boolean, default=False)
    """Whether this directory is the root specified in the settings
        (not necessarily the root on Google Drive)"""

    __mapper_args__ = {
        'polymorphic_identity': 'directory'
    }

    _children: list[LocalFile]

    @property
    def children(self) -> list[LocalFile] | None:
        """Subdirectories and regular files in this directory.

        Returns:
            a list of the directory's children
        """
        return self._children or []

    def flatten(self) -> dict[str, LocalFile]:
        """Convert the tree to a flat dictionary.

        Returns:
            a flat dictionary where files are mapped by their ids
        """
        d: dict[str, LocalFile] = {self.id: self}
        for c in self.children or []:
            d.update(c.flatten() if isinstance(c, LocalDirectory)
                     else {c.id: c})
        return d

    @overrides
    def as_dict(self, depth: int = maxsize,
                remove_gdfile_id: bool = False) -> dict[str, Any]:
        d = super().as_dict(depth, remove_gdfile_id)
        if depth > 0:
            d['children'] = [c.as_dict(depth - 1, remove_gdfile_id)
                             for c in self._children]
        return d

    @overrides
    def print_tree(self, _pfx: list[bool] | None = None) -> None:
        """Print the tree file names to standard output (for debugging)."""
        if _pfx is None:
            _pfx = []
        super().print_tree(_pfx)
        for child in (self.children or [])[:-1]:
            child.print_tree(_pfx + [True])
        if self.children:
            self.children[-1].print_tree(_pfx + [False])


class LocalFileRevision(Base):
    """Represents a local version of a file downloaded from Google Drive."""

    __tablename__ = 'gdfilerev'

    gdfilerev_id: int = Column(Integer, primary_key=True)
    """Revision id (used internally by the database engine)"""

    file_id: str = Column(String, ForeignKey(LocalRegularFile.id))
    """File id (generated by Google Drive)"""

    file: LocalRegularFile = relationship(
        LocalRegularFile, viewonly=True,
        backref=backref('_revisions'), remote_side=[LocalRegularFile.id]
    )

    name: str = Column(String)
    """File name (without directory)"""

    revision_id: str = Column(String, nullable=False)
    """Revision id (generated by Google Drive)"""

    can_download: bool = Column(Boolean, nullable=False)
    """Whether file can be downloaded"""

    modified_time: datetime = Column(_TimestampWithTZ())
    """Last modification timestamp"""

    modified_by = Column(String)
    """Name of the user who made the last change"""

    mime_type: str | None = Column(String)
    """MIME type"""

    size: int = Column(BigInteger)
    """File size in bytes"""

    md5_checksum: str = Column(String)
    """File hash"""

    @property
    def local_name(self) -> str:
        """Local file name (without path).

        Returns:
            Local file name
        """
        return self.file_id + '_' + self.revision_id + '_' + self.md5_checksum

    obsolete_since: datetime = Column(_TimestampWithTZ(), nullable=True)

    __table_args__ = (UniqueConstraint('file_id', 'revision_id',
                      name='_file_revision_unique'),
                      )


class DatabaseMetadata(Base):
    """Represents metadata such as the program version."""

    __tablename__ = '_db_metadata'

    _db_metadata_id: int = Column(Integer, primary_key=True)
    """Internal id used by the database engine."""

    key: str = Column(String, unique=True)
    """Metadata key"""

    value: str = Column(String)
    """Metadata value corresponding to key"""

    def __repr__(self):
        return f'[{self.key} = {self.value}]'


class LocalStorageDatabase:
    """Interacts with a database that stores file metadata."""

    def __init__(self, db_config: dict[str, str]):
        """
        Args:
            db_config: database configuration

        For a description of the expected keys and values of `db_config`,
        see the section ``GoogleDrive`` in
        :download:`the documentation <../README_CONFIG.md>`.
        """  # noqa:D205,D400
        self.db_config: dict[str, str] = db_config
        self._engine = self.__create_engine()
        Base.metadata.create_all(self._engine)
        self.upgrade()

    def __create_engine(self) -> Engine:
        cfg = self.db_config
        driver = cfg.get('Driver', '')
        url = sqlalchemy_url.create(
            cfg['Dialect'] + ('+' if driver else '') + driver,
            username=cfg.get('Username', None),
            password=cfg.get('Password', None),
            host=cfg.get('Host', None),
            port=int(p) if (p := cfg.get('Port', None)) else None,
            database=cfg.get('Database', None),
            query=parse_qs(cfg.get('Query', ''))
        )
        return create_engine(url)

    def upgrade(self) -> None:
        """Upgrade database to the current model version. Currently unused."""
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

    def get_tree(self, session: Session) -> LocalDirectory | None:
        """Return an object representing the root of the local file tree.

        The files and subdirectories can be accessed in the
        `children` attribute as long as the `session` is still open.

        Args:
            session: the database session

        Returns:
            an object representing the root of the local
            file tree (which is a snapshot of the contents stored in
            Google Drive)
        """
        logger.info('requesting local tree')
        stmt = select(LocalDirectory).where(  # type: ignore
            LocalDirectory.is_root)
        result = session.execute(stmt)
        root = result.scalars().first()
        return root

    def get_file_by_id(self, session: Session, id: str) -> LocalFile | None:
        """Return an object representing a specific file stored locally.

        It can be a regular file or a directory.

        Args:
            session: the database session
            id: the id of the file or directory

        Returns:
            an object representing the file with the specified id,
            or `None` if it does not exist
        """
        log = logger.bind(id=id)
        log.info('requesting local file')
        stmt = select(LocalFile).where(LocalFile.id == id)  # type: ignore
        result = session.execute(stmt)
        f = result.scalars().first()
        log.info('requested local file', found=bool(f))
        return f

    def new_session(self) -> Session:
        """Create a new database session.

        Returns:
            a new database session
        """
        return Session(self._engine)

    def get_obsolete_file_revisions(self, session: Session,
                                    until: datetime | None = None) \
            -> list[LocalFileRevision]:
        """Return file revisionss marked for deletion before a given instant.

        This method only applies to regular files. The returned files
        have been either deleted or overwritten with newer versions on
        Google Drive.

        Args:
            session: the database session
            until: if set, only files that have marked for deletion up to
                the specified instant will be returned

        Returns:
            a (possibly empty) list of objects representing the files
            that have been marked for deletion until the time set by `until`
        """
        logger.info('requesting list of files to delete', until=until)
        stmt = select(LocalFileRevision).where(  # type: ignore
            LocalFileRevision.obsolete_since.is_not(None))
        if until:
            stmt = stmt.where(LocalFileRevision.obsolete_since <= until)
        result = session.execute(stmt)
        return result.scalars().all()
