"""Has a class that interacts with a database that stores file metadata."""

from __future__ import annotations

from datetime import datetime
from typing import TypeVar, Type
from urllib.parse import parse_qs

from packaging.version import parse as parse_version
from sqlalchemy import create_engine, select, update
from sqlalchemy.engine import Engine, URL
from sqlalchemy.orm import Session
from structlog import getLogger

from blabgddatalake import __version__
from blabgddatalake.config import DatabaseConfig
from blabgddatalake.local import Base, DatabaseMetadata
from blabgddatalake.local.file import LocalFile, LocalDirectory
from blabgddatalake.local.regularfile import LocalFileRevision
from blabgddatalake.local.gwfile import LocalExportedGWFileVersion

_logger = getLogger(__name__)


class LocalStorageDatabase:
    """Interacts with a database that stores file metadata."""

    def __init__(self, db_config: DatabaseConfig):
        """
        Args:
            db_config: database configuration.
        """  # noqa:D205,D400
        self.db_config = db_config
        self._engine = self.__create_engine()
        Base.metadata.create_all(self._engine)
        self.upgrade()

    def __create_engine(self) -> Engine:
        driver = self.db_config.driver
        url = URL.create(
            self.db_config.dialect + ('+' if driver else '') + driver,
            username=self.db_config.username,
            password=self.db_config.password,
            host=self.db_config.host,
            port=self.db_config.port,
            database=self.db_config.database,
            query=parse_qs(self.db_config.query or '')
        )
        return create_engine(url)

    def upgrade(self) -> None:
        """Upgrade database to the current model version. Currently unused."""
        with Session(self._engine) as session:
            stmt = select(DatabaseMetadata).where(
                DatabaseMetadata.key == 'version')
            result = session.execute(stmt)
            log = _logger.bind(new=False)
            if (version_row := result.first()):
                version = version_row[0].value
            else:
                version = __version__
                log = _logger.bind(new=True)
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
                _logger.info('upgrading database version',
                             old=current_version, new=v)
                with Session(self._engine) as session:
                    fn()
                    session.execute(
                        update(DatabaseMetadata).where(
                            DatabaseMetadata.key == 'version')
                        .values(value=version)
                    )
                    session.commit()
                    current_version = v

    @classmethod
    def get_tree(cls, session: Session) -> LocalDirectory | None:
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
        _logger.info('requesting local tree')
        stmt = select(LocalDirectory).where(LocalDirectory.is_root)
        result = session.execute(stmt)
        root = result.scalars().first()
        return root

    @classmethod
    def get_file_by_id(cls, session: Session, file_id: str,
                       include_obsolete: bool = False) -> LocalFile | None:
        """Return an object representing a specific file stored locally.

        It can be a regular file or a directory.

        Args:
            session: the database session
            file_id: the id of the file or directory
            include_obsolete: whether a file marked for deletion should be
                returned

        Returns:
            an object representing the file with the specified id,
            or `None` if it does not exist
        """
        log = _logger.bind(id=file_id)
        log.info('requesting local file')
        stmt = select(LocalFile).where(LocalFile.id == file_id)
        if not include_obsolete:
            stmt = stmt.where(LocalFile.obsolete_since.is_(None))
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

    T = TypeVar('T', LocalFile, LocalFileRevision, LocalExportedGWFileVersion)

    @classmethod
    def _get_obsolete_items(cls, c: Type[T], session: Session,
                            until: datetime | None = None) -> list[T]:
        stmt = select(c).where(c.obsolete_since.is_not(None))
        if until:
            stmt = stmt.where(c.obsolete_since <= until)
        result = session.execute(stmt)
        return result.scalars().all()

    @classmethod
    def get_obsolete_file_revisions(cls, session: Session,
                                    until: datetime | None = None) \
            -> list[LocalFileRevision]:
        """Return file revisions marked for deletion before a given instant.

        This method only applies to regular files. The returned files
        have been either deleted or overwritten with newer versions on
        Google Drive.

        Args:
            session: the database session
            until: if set, only files that have marked for deletion up to
                the specified instant will be returned

        Returns:
            a list of objects representing the files
            that have been marked for deletion until the time set by `until`
        """
        _logger.info('requesting file revisions to delete', until=until)
        return cls._get_obsolete_items(LocalFileRevision, session, until)

    @classmethod
    def get_obsolete_gw_file_versions(cls, session: Session,
                                      until: datetime | None = None) \
            -> list[LocalExportedGWFileVersion]:
        """Return file versions marked for deletion before a given instant.

        This method only applies to Google Workspace files. The returned files
        have been either deleted or overwritten with newer versions on
        Google Drive.

        Args:
            session: the database session
            until: if set, only files that have marked for deletion up to
                the specified instant will be returned

        Returns:
            a list of objects representing the files
            that have been marked for deletion until the time set by `until`
        """
        _logger.info('requesting GW file versions to delete', until=until)
        return cls._get_obsolete_items(LocalExportedGWFileVersion, session,
                                       until)

    @classmethod
    def get_obsolete_files(cls, session: Session,
                           until: datetime | None = None) \
            -> list[LocalFile]:
        """Return files and folders marked for deletion before a given instant.

        This method applies to regular files, directories and
        Google Workspace files. The returned files
        have been either deleted or overwritten with newer versions on
        Google Drive.

        Args:
            session: the database session
            until: if set, only files that have marked for deletion up to
                the specified instant will be returned

        Returns:
            a list of objects representing the files
            that have been marked for deletion until the time set by `until`
        """
        _logger.info('requesting files to delete (only metadata)', until=until)
        return cls._get_obsolete_items(LocalFile, session, until)
