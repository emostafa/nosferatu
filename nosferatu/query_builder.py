
# TODO: create test database on the fly for the purpose of testing
class QueryBuilder(object):
    """Query is a builder for SQL query statements"""

    def __init__(self):
        self._fields = []
        self._table_name = None
        self._schema_name = None
        self._wheres = []
        self._order_by = None
        self._limit = None
        self._offset = None

    def select(self, fields):
        raise NotImplementedError

    def set_table_name(self, schema_name: str, table_name: str):
        self._schema_name = schema_name
        self._table_name = table_name

    def where(self, stmt):
        self._wheres.append(stmt)

    def order_by(self, stmt):
        self._order_by = "ORDER BY %s" % stmt

    def limit(self, num):
        self._limit = "LIMIT %d" % num

    def offset(self, num):
        self._offset = "OFFSET %d" % num

    def build(self) -> str:
        q = "SELECT "

        if len(self._fields) > 0:
            q += ','.join(self._fields)
        else:
            q += '*'

        q += " FROM %s.%s" % (self._schema_name, self._table_name)

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

    def clear(self):
        self._fields = []
        self._table_name = None
        self._schema_name = None
        self._wheres = []
        self._order_by = None
        self._limit = None
        self._offset = None