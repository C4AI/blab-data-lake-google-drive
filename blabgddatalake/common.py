"""Contains some useful components used by both local and remote modules."""

from typing import Any, Sequence

from overrides import overrides


class TreeNode:
    """Represents a node of a tree (e.g. a file or directory)."""

    name: str
    """Node name (e.g. file name)"""

    def print_tree(self, _pfx: list[bool] | None = None) -> None:
        """Print the tree file names to standard output (for debugging)."""
        if _pfx is None:
            _pfx = []
        for p in _pfx[:-1]:
            print(' ┃ ' if p else '   ', end=' ')
        if _pfx:
            print(' ┠─' if _pfx[-1] else ' ┖─', end=' ')
        print(self.name)


class NonLeafTreeNode(TreeNode):
    """Represents a node of a tree (e.g. a file or directory)."""

    children: Any  # list of TreeNode
    """list of children (:class:`TreeNode`)"""

    @overrides
    def print_tree(self, _pfx: list[bool] | None = None) -> None:
        super().print_tree(_pfx)
        if _pfx is None:
            _pfx = []
        if not isinstance(self.children, list):
            return
        for child in self.children[:-1]:
            if isinstance(child, TreeNode):
                child.print_tree(_pfx + [True])
        if self.children and isinstance(last := self.children[-1], TreeNode):
            last.print_tree(_pfx + [False])


__all__: Sequence[str] = [c.__name__ for c in [
    NonLeafTreeNode,
    TreeNode,
]]
