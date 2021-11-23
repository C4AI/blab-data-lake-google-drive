import logging
import sqlalchemy
from sqlalchemy import Column, ForeignKey, Integer, String, TIMESTAMP, \
    select, update
from sqlalchemy.orm import declarative_base, Session
from typing import Dict
from urllib.parse import parse_qs
from packaging.version import parse as parse_version

from . import VERSION


Base = declarative_base()


class StoredLakeFile(Base):

    __tablename__ = 'gdfile'

    gdfile_id = Column(Integer, primary_key=True)
    id = Column(String, unique=True)
    name = Column(String)
    created_time = Column(TIMESTAMP(timezone=True))
    modified_time = Column(TIMESTAMP(timezone=True))
    modified_by = Column(String)
    web_url = Column(String)
    parent_id = Column(String, ForeignKey(id))


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
        self.engine = self.__get_engine()
        self.meta = sqlalchemy.MetaData()
        Base.metadata.create_all(self.engine)
        self.upgrade()

    def __get_engine(self) -> sqlalchemy.engine.base.Engine:
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
        with Session(self.engine) as session:
            stmt = select(StoredDatabaseMetadata).where(  # type: ignore
                StoredDatabaseMetadata.key == 'version')
            result = session.execute(stmt)
            if (version_row := result.first()):
                version = version_row[0].value
                logging.info(f'Database version: {version}')
            else:
                version = VERSION
                row = StoredDatabaseMetadata(key='version', value=version)
                session.add(row)
                session.commit()
                logging.info(f'Database version (new): {version}')

        def upgrade_1_0_0() -> None:
            pass

        upgraders = {
            '1.0.0': upgrade_1_0_0,
        }

        current_version = parse_version(version)
        for version, fn in upgraders.items():
            v = parse_version(version)
            if current_version < v:
                logging.info(f'Upgrading database to version {version}')
                with Session(self.engine) as session:
                    fn()
                    session.execute(
                        update(StoredDatabaseMetadata).where(
                            StoredDatabaseMetadata.key == 'version').
                        values(value=version))
                    session.commit()
                    current_version = v
