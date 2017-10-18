"""Nosferatu"""
import copy
import psycopg2

from .query_builder import QueryBuilder


class Nosferatu(object):
    """Nosfertau"""

    @staticmethod
    def connect(config):
        """connect to a postgresql using config"""
        c = psycopg2.connect(host=config['host'],
                             user=config['username'],
                             password=config['password'],
                             dbname=config['database'])
        return Database(c.cursor())


class Column(object):
    """Column represents a column in a table definition"""

    def __init__(self, name):
        self.name = name


class Table(object):
    """Table represents a table definition in a schema"""

    def __init__(self, cursor, name, schema_name):
        self.name = name
        self.schema_name = schema_name
        self.cursor = cursor
        self.columns = []
        self._query_builder = QueryBuilder()
        self._query_builder.set_table_name(schema_name, name)
        # TOFIX: Lazy load columns lookup
        self.lookup_columns()

    def __str__(self):
        return 'Table "%s"' % self.name

    def lookup_columns(self):
        """lookup columns in the given table and push them to self.columns"""
        sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '%s'
        """ % self.name
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        for row in rows:
            col = Column(row[0])
            self.columns.append(col)

    def list_columns(self):
        return map(lambda x: x.name, self.columns)

    def all(self):
        """all executes the query and return result"""
        q = self._query_builder.build()
        self._query_builder.clear()
        self.cursor.execute(q)
        return self.cursor.fetchall()

    def one(self):
        """all executes the query and return only one result"""
        raise NotImplementedError

    def filter(self, stmt):
        """filter the desired results
            e.g: db.table_name.filter("age = 15")
        """
        self._query_builder.where(stmt)
        return self


class Database(object):
    """Database represents a database which can be used to access
        tables then columns or table methods"""

    def __init__(self, cursor):
        self.cursor = cursor
        self.tables = []
        self.lookup_tables()
        self.schemas = []

    def lookup_tables(self):
        """lookup tables in the given database and push them to __dict__"""
        sql = "SELECT table_name, table_schema FROM information_schema.tables"
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        for row in rows:
            if row[0] == 'tables':
                continue
            # self.schemas.append(row[1])
            t = Table(self.cursor, row[0], row[1])
            self.tables.append(t)
            self.__setattr__(row[0], t)

    def __getatrr__(self, name):
        if name in self.__dict__:
            return copy.copy(self.__dict__[name])
        return None
