import logging
import sqlalchemy
from sqlalchemy import Integer, String, Boolean, TIMESTAMP, BigInteger, \
    Column, ForeignKey, select, update
from sqlalchemy.orm import declarative_base, Session, relationship, backref
from typing import Dict, List
from urllib.parse import parse_qs
from packaging.version import parse as parse_version

from . import VERSION


logger = logging.getLogger(__package__)


Base = declarative_base()


class StoredRemoteFile(Base):

    __tablename__ = 'gdfile'

    gdfile_id = Column(Integer, primary_key=True)
    id = Column(String, unique=True)
    name = Column(String)
    created_time = Column(TIMESTAMP(timezone=True))
    modified_time = Column(TIMESTAMP(timezone=True))
    modified_by = Column(String)
    web_url = Column(String)
    is_root = Column(Boolean, default=False)
    mime_type = Column(String)
    size = Column(BigInteger)
    md5_checksum = Column(String)

    parent_id = Column(String, ForeignKey(id))
    parent: 'StoredRemoteFile' = relationship(
        'StoredRemoteFile',
        backref=backref('reports'), remote_side=[id]
    )


class StoredDatabaseMetadata(Base):

    __tablename__ = '_db_metadata'

    _db_metadata_id = Column(Integer, primary_key=True)
    key = Column(String, unique=True)
    value = Column(String)

    def __repr__(self):
        return f'[{self.key} = {self.value}]'


class MetadataCacheDatabase:

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
            stmt = select(StoredDatabaseMetadata).where(  # type: ignore
                StoredDatabaseMetadata.key == 'version')
            result = session.execute(stmt)
            if (version_row := result.first()):
                version = version_row[0].value
                logger.info(f'Database version: {version}')
            else:
                version = VERSION
                row = StoredDatabaseMetadata(key='version', value=version)
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
                        update(StoredDatabaseMetadata).where(
                            StoredDatabaseMetadata.key == 'version').
                        values(value=version))
                    session.commit()
                    current_version = v

    def get_tree(self) -> List[StoredRemoteFile]:
        return []  # TODO

    def new_session(self) -> Session:
        return Session(self._engine)
