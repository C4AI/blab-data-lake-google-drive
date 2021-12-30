"""Contains a class that represents a regular file from Google Drive."""
from __future__ import annotations

from datetime import datetime
from overrides import overrides
from sqlalchemy import (
    Column, String, ForeignKey, Integer, Boolean,
    BigInteger, UniqueConstraint,
)
from sqlalchemy.orm import relationship, backref
from sys import maxsize
from typing import Any

from blabgddatalake.local import Base, _TimestampWithTZ
from blabgddatalake.local.file import LocalFile


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
    def can_download(self) -> bool:
        """Whether file can be downloaded.

        Returns:
            ``True`` if the file can be downloaded, ``False`` otherwise
        """
        return self.head_revision.can_download

    @property
    def local_name(self) -> str:
        """Local file name (without path).

        Returns:
            Local file name
        """
        return (self.id +
                '_' + (self.head_revision_id or '') +
                '_' + (self.md5_checksum or ''))

    @property
    def size(self) -> int:
        """Size in bytes.

        Returns:
            Size of the current revision of the file
        """
        return self.head_revision.size

    head_revision_id: str | None = Column(
        String, ForeignKey('gdfilerev.revision_id'))
    """Current revision id (generated by Google Drive)"""

    head_revision: LocalFileRevision = relationship(
        'LocalFileRevision', uselist=False,
        foreign_keys=[LocalFile.id, head_revision_id]
    )
    """Current revision"""

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

    @overrides
    def as_dict(self, depth: int = maxsize,
                remove_gdfile_id: bool = False) -> dict[str, Any]:
        d = super().as_dict(depth, remove_gdfile_id)
        d['can_download'] = self.can_download
        d['size'] = self.size
        d['md5_checksum'] = self.md5_checksum
        return d


class LocalFileRevision(Base):
    """Represents a local version of a file downloaded from Google Drive."""

    __tablename__ = 'gdfilerev'

    gdfilerev_id: int = Column(Integer, primary_key=True)
    """Revision id (used internally by the database engine)"""

    file_id: str = Column(String, ForeignKey(LocalRegularFile.id))
    """File id (generated by Google Drive)"""

    file: LocalRegularFile = relationship(
        LocalRegularFile, viewonly=True,
        foreign_keys=[file_id],
        backref=backref('_revisions')
    )
    """File this revision belongs to"""

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
    """Instant when the deletion of the file was detected

    It is ``None`` for files that have not been deleted.
    """

    __table_args__ = (UniqueConstraint('file_id', 'revision_id',
                                       name='_file_revision_unique'),
                      )