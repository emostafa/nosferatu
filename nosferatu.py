"""Nosferatu"""
import copy
import psycopg2


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


class QueryBuilder(object):
    """Query is a builder for SQL query statements"""

    def __init__(self):
        self._fields = []
        self._table_name = None
        self._wheres = []
        self._order_by = None
        self._limit = None
        self._offset = None

    def select(self, fields):
        raise NotImplementedError

    def set_table_name(self, table_name):
        self._table_name = table_name

    def where(self, stmt):
        self._wheres.append(stmt)

    def order_by(self, stmt):
        self._order_by = "ORDER BY %s" % stmt

    def limit(self, num):
        self._limit = "LIMIT %d" % num

    def offset(self, num):
        self._offset = "OFFSET %d" % num

    def build(self):
        q = "SELECT "

        if len(self._fields) > 0:
            q += ','.join(self._fields)
        else:
            q += '*'

        q += " FROM %s" % self._table_name

        if len(self._wheres) > 0:
            q += " WHERE"
            for stmt in self._wheres:
                q += " %s" % stmt

        if self._order_by is not None:
            q += " %s" % self._order_by

        if self._limit is not None:
            q += " %s" % self._limit

        if self._offset is not None:
            q += " %s" % self._offset

        return q


class Column(object):
    """Column represents a column in a table defention"""

    def __init__(self, name):
        self.name = name


class Table(object):
    """Table represents a table defenition in a schema"""

    def __init__(self, name, cursor):
        self.name = name
        self.cursor = cursor
        self.columns = []
        self._query_builder = QueryBuilder()
        self._query_builder.set_table_name(name)
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
        """ % (self.name)
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        for row in rows:
            col = Column(row[0])
            self.columns.append(col.name)

    def inspect(self):
        for col in self.columns:
            print col

    def all(self):
        """all executes the query and return result"""
        raise NotImplementedError

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

    def lookup_tables(self):
        """lookup tables in the given database and push them to __dict__"""
        sql = "SELECT table_name FROM information_schema.tables"
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        for row in rows:
            if row[0] == 'tables':
                continue
            self.tables.append(row[0])
            self.__setattr__(row[0], Table(row[0], self.cursor))

    def __getatrr__(self, name):
        if name in self.__dict__:
            return copy.copy(self.__dict__[name])
        return None
