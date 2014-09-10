import six

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.io.libevreactor import LibevConnection
from cassandra.query import dict_factory, Statement, SimpleStatement

from cqlengine.statements import BaseCQLStatement


class AsyncSession(object):

    def __init__(self, hosts=None, keyspace=None, load_balancing_policy=None,
                 control_connection_timeout=10, executor_threads=10, session_timeout=120):
        """ Connect to cassandra cluster """
        cluster = Cluster(hosts or [],
                          control_connection_timeout=control_connection_timeout,
                          executor_threads=executor_threads,
                          load_balancing_policy=load_balancing_policy)

        cluster.connection_class = LibevConnection
        self.session = cluster.connect(keyspace=keyspace)
        self.session.row_factory = dict_factory
        self.session.default_timeout = session_timeout
        self.on_error = lambda x: None
        self.on_success = lambda x: None

    def execute(self, query, params=None, consistency_level=None):
        raise NotImplementedError

    def _execute(self, query, params=None, consistency_level=None):
        """ Run CQL query async"""
        if consistency_level is None:
            consistency_level = ConsistencyLevel.ONE

        if isinstance(query, Statement):
            pass

        elif isinstance(query, BaseCQLStatement):
            params = query.get_context()
            query = str(query)
            query = SimpleStatement(query, consistency_level=consistency_level)

        elif isinstance(query, six.string_types):
            query = SimpleStatement(query, consistency_level=consistency_level)

        params = params or {}

        cassandra_future = self.session.execute_async(query, parameters=params)
        cassandra_future.add_callbacks(self.on_success, self.on_error)