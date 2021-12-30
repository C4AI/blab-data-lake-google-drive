from overrides import overrides
from typing import Any


class TreeNode:
    name: str

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
    children: Any  # list of TreeNode

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
