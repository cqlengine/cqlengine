#http://pypi.python.org/pypi/cql/1.0.4
#http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2 /
#http://cassandra.apache.org/doc/cql/CQL.html

from collections import namedtuple
import Queue

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra import decoder
from cassandra.query import SimpleStatement
import logging

from cqlengine.exceptions import CQLEngineException

from contextlib import contextmanager

LOG = logging.getLogger('cqlengine.cql')

Host = namedtuple('Host', ['name', 'port'])

_max_connections = 10

# global connection pool
cluster = None
connection_pool = None


class CQLConnectionError(CQLEngineException): pass


class RowResult(tuple):
    pass


def _column_tuple_factory(colnames, values):
    return tuple(colnames), [RowResult(v) for v in values]


def setup(hosts, username=None, password=None, max_connections=10, default_keyspace=None, consistency='ONE'):
    """
    Records the hosts and connects to one of them

    :param hosts: list of hosts, strings in the <hostname>:<port>, or just <hostname>
    """
    global _max_connections
    global cluster
    global connection_pool
    _max_connections = max_connections

    if default_keyspace:
        from cqlengine import models
        models.DEFAULT_KEYSPACE = default_keyspace

    _hosts = []
    for host in hosts:
        host = host.strip()
        host = host.split(':')
        if len(host) == 1:
            _hosts.append(Host(host[0], 9160))
        elif len(host) == 2:
            _hosts.append(Host(*host))
        else:
            raise CQLConnectionError("Can't parse {}".format(''.join(host)))

    if not _hosts:
        raise CQLConnectionError("At least one host required")

    cluster = Cluster([h.name for h in _hosts])
    connection_pool = ConnectionPool(_hosts, username, password, consistency)


class ConnectionPool(object):
    """Handles pooling of database connections."""

    def __init__(self, hosts, username=None, password=None, consistency=None):
        self._hosts = hosts
        self._username = username
        self._password = password
        self._consistency = consistency

        self._queue = Queue.Queue(maxsize=_max_connections)

    def clear(self):
        """
        Force the connection pool to be cleared. Will close all internal
        connections.
        """
        try:
            while not self._queue.empty():
                self._queue.get().shutdown()
        except:
            pass

    def get(self):
        """
        Returns a usable database connection. Uses the internal queue to
        determine whether to return an existing connection or to create
        a new one.

        :rtype: cassandra.cluster.Session
        """
        try:
            if self._queue.empty():
                return self._create_connection()
            return self._queue.get()
        except CQLConnectionError as cqle:
            raise cqle

    def put(self, conn):
        """
        Returns a session into the queue, freeing it up for other queries to
        use.

        :param conn: The connection to be released
        :type conn: cassandra.cluster.Session
        """

        if self._queue.full():
            conn.shutdown()
        else:
            self._queue.put(conn)

    def _create_connection(self):
        """ Creates and returns a new session object """
        session = cluster.connect()
        return session

    def execute(self, query, params, consistency=None, row_factory=_column_tuple_factory):
        statement = SimpleStatement(query)
        statement.consistency_level = consistency or self._consistency
        if isinstance(statement.consistency_level, basestring):
            statement.consistency_level = ConsistencyLevel.name_to_value[statement.consistency_level]

        try:
            session = self.get()
            session.row_factory = row_factory
            results = session.execute(statement, params)
            self.put(session)
            return results
        except decoder.ConfigurationException as ex:
            #TODO: add in more robust error handling
            raise CQLEngineException(ex.message)


def execute(query, params={}, row_factory=_column_tuple_factory):
    return connection_pool.execute(query, params, row_factory=row_factory)

# FIXME: deprecated
@contextmanager
def connection_manager():
    """
    DEPRECATED

    This is no longer neccesary since we're not
    passing cursors around, leaving it in for
    backwards compatibility

    :rtype: ConnectionPool
    """
    yield connection_pool
