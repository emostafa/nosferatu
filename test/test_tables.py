import os
import unittest
import collections

from nosferatu.nosferatu import Nosferatu


class TestTables(unittest.TestCase):

    def setUp(self):
        host = os.env.get('NOSFERATU_HOST', 'localhost')
        username = os.env.get('NOSFERATU_DB_USER', 'postgres')
        password = os.env.get('NOSFERATU_DB_PASS')
        db_name = os.env.get('NOSFERATU_DB_NAME', 'postgres')
        self.db = Nosferatu.connect({'host': host,
                                     'username': username,
                                     'password': password,
                                     'database': db_name})
        self.tables = self.db.tables

    def test_tables(self):
        assert len(self.tables) > 0

    def test_tables_have_columns(self):
        assert len(self.tables[0].columns) > 0

    def test_list_columns(self):
        try:
            cols = self.tables[0].list_columns()
        except Exception as e:
            self.fail("list_columns() raised an exception")
        assert isinstance(cols, collections.Iterable)

    def test_query_all(self):
        rows = self.tables[1].all()
        assert isinstance(rows, collections.Iterable)
        assert len(list(rows)) > 0

    def test_query_one(self):
        rows = self.tables[0].filter('id=1').one()
        assert not isinstance(rows, collections.Iterable)

    def test_query_filter_all(self):
        rows = self.tables[0].filter('id=1').all()
        assert isinstance(rows, collections.Iterable)
        assert len(rows) > 0

    def test_query_filter_one(self):
        row = self.tables[0].filter('id=1').one()
        assert row is not None

    def tearDown(self):
        # self.db.Disconnect()
        pass
