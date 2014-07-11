#http://pypi.python.org/pypi/cql/1.0.4
#http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2 /
#http://cassandra.apache.org/doc/cql/CQL.html

from collections import namedtuple
from cassandra.cluster import Cluster
from cassandra.policies import HostDistance
from cassandra.query import SimpleStatement, Statement

try:
    import Queue as queue
except ImportError:
    # python 3
    import queue

import logging

from cqlengine.exceptions import CQLEngineException, UndefinedKeyspaceException
from cassandra import ConsistencyLevel
from cqlengine.statements import BaseCQLStatement
from cassandra.query import dict_factory

LOG = logging.getLogger('cqlengine.cql')

class CQLConnectionError(CQLEngineException): pass

Host = namedtuple('Host', ['name', 'port'])

cluster = None
session = None
lazy_connect_args = None
default_consistency_level = None

def setup(
        hosts,
        default_keyspace,
        consistency=ConsistencyLevel.ONE,
        lazy_connect=False,
        skip_schema_loading=False,
        one_core_connection=False,
        **kwargs):
    """
    Records the hosts and connects to one of them

    :param hosts: list of hosts, see http://datastax.github.io/python-driver/api/cassandra/cluster.html
    :type hosts: list
    :param default_keyspace: The default keyspace to use
    :type default_keyspace: str
    :param consistency: The global consistency level
    :type consistency: int
    :param lazy_connect: True if should not connect until first use
    :type lazy_connect: bool
    :param skip_schema_loading: True if should not load schema after connect. Will make connects faster. Will break prepared statements, and if you make a schema change, you won't properly wait for schema agreement.
    :type skip_schema_loading: bool
    :param one_core_connection: True if only one connection per host instead of 2
    :type one_core_connection: bool
    """
    global cluster, session, default_consistency_level, lazy_connect_args

    if 'username' in kwargs or 'password' in kwargs:
        raise CQLEngineException("Username & Password are now handled by using the native driver's auth_provider")

    if not default_keyspace:
        raise UndefinedKeyspaceException()

    from cqlengine import models
    models.DEFAULT_KEYSPACE = default_keyspace

    default_consistency_level = consistency
    if lazy_connect:
        lazy_connect_args = (hosts, default_keyspace, consistency, kwargs)
        return

    cluster = Cluster(hosts, **kwargs)
    if skip_schema_loading:
        cluster.control_connection._refresh_schema = lambda *args, **kwargs: None
    if one_core_connection:
        cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
    session = cluster.connect()
    session.row_factory = dict_factory

def execute(query, params=None, consistency_level=None):

    handle_lazy_connect()
    
    if not session:
        raise CQLEngineException("It is required to setup() cqlengine before executing queries")


    if consistency_level is None:
        consistency_level = default_consistency_level

    if isinstance(query, Statement):
        pass

    elif isinstance(query, BaseCQLStatement):
        params = query.get_context()
        query = str(query)
        query = SimpleStatement(query, consistency_level=consistency_level)

    elif isinstance(query, basestring):
        query = SimpleStatement(query, consistency_level=consistency_level)



    params = params or {}
    result = session.execute(query, params)

    return result


def get_session():
    handle_lazy_connect()
    return session

def get_cluster():
    handle_lazy_connect()
    return cluster

def handle_lazy_connect():
    global lazy_connect_args
    if lazy_connect_args:
        hosts, default_keyspace, consistency, kwargs = lazy_connect_args
        lazy_connect_args = None
        setup(hosts, default_keyspace, consistency, **kwargs)
