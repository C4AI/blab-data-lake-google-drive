"""Contains classes that represent a file or directory from Google Drive."""

from __future__ import annotations

from datetime import datetime
from sys import maxsize
from typing import Any

from overrides import overrides
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, inspect
from sqlalchemy.orm import backref, relationship

import blabgddatalake.common as common
import blabgddatalake.local as local

_TimestampWithTZ = local.TimestampWithTZ


class LocalFile(local.Base, common.TreeNode):
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

    parent: LocalDirectory = relationship('LocalDirectory',
                                          backref=backref('_children'),
                                          remote_side=[id])
    """Parent directory, or ``None`` if this is the root"""

    modified_time: datetime = Column(_TimestampWithTZ())
    """Last modification timestamp"""

    modified_by = Column(String)
    """Name of the user who made the last change"""

    mime_type: str | None = Column(String)
    """MIME type"""

    obsolete_since: datetime = Column(_TimestampWithTZ(), nullable=True)
    """Instant when the deletion of the file was detected

    It is ``None`` for files that have not been deleted.
    """

    type: str = Column(String)
    """Used internally to identify the file type."""

    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'generic file'
    }

    def __repr__(self) -> str:
        return f'(name={self.name}, gdid={self.gdfile_id})'

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

    def as_dict(self,
                depth: int = maxsize,
                remove_gdfile_id: bool = False) -> dict[str, Any]:
        """Prepare the object to be serialised by converting it to a dict.

        Args:
            depth: maximum depth
            remove_gdfile_id: remove internal id

        Returns:
            a dictionary with the object data
        """
        cols = [c_attr.key for c_attr in inspect(self).mapper.column_attrs]
        d = {c: getattr(self, c) for c in cols}
        d['virtual_path'] = self.virtual_path
        d.pop('obsolete_since', None)
        if remove_gdfile_id:
            del d['gdfile_id']
        return d


class LocalDirectory(LocalFile, common.NonLeafTreeNode):
    """Represents a Google Drive directory."""

    is_root: bool = Column(Boolean, default=False)
    """Whether this directory is the root specified in the settings
        (not necessarily the root on Google Drive)"""

    __mapper_args__ = {'polymorphic_identity': 'directory'}

    _children: list[LocalFile]

    @property
    def children(self) -> list[LocalFile] | None:
        """Subdirectories and regular files in this directory.

        Returns:
            a list of the directory's children
        """
        return list(filter(lambda c: c.obsolete_since is None, self._children))

    def flatten(self) -> dict[str, LocalFile]:
        """Convert the tree to a flat dictionary.

        Returns:
            a flat dictionary where files are mapped by their ids
        """
        d: dict[str, LocalFile] = {self.id: self}
        for c in self.children or []:
            d.update(
                c.flatten() if isinstance(c, LocalDirectory) else {c.id: c})
        return d

    @overrides
    def as_dict(self,
                depth: int = maxsize,
                remove_gdfile_id: bool = False) -> dict[str, Any]:
        d = super().as_dict(depth, remove_gdfile_id)
        if depth > 0:
            d['children'] = [
                c.as_dict(depth - 1, remove_gdfile_id) for c in self._children
            ]
        return d
