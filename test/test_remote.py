import unittest
from dataclasses import replace
from test import BaseTest
from test.gdmock import GDServiceMock


class RemoteTest(BaseTest):

    def test_initial(self) -> None:
        """Conversion from the API-provided data to the classes in `remote`."""
        gdservice = GDServiceMock(self.config.google_drive, self.http)
        self.check_equal_tree(self.all_files_by_id, gdservice.get_tree())

    def test_small_pages(self) -> None:
        """Similar to `test_initial`, but with requests split into pages."""
        for i in [1, 2, 5]:
            with self.subTest(page_size=i):
                gdservice = GDServiceMock(
                    replace(self.config.google_drive, page_size=i), self.http)
                self.check_equal_tree(self.all_files_by_id,
                                      gdservice.get_tree())


if __name__ == '__main__':
    unittest.main()
