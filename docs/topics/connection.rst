==========
Connection
==========

**Users of versions < 0.4, please read this post before upgrading:** `Breaking Changes`_

.. _Breaking Changes: https://groups.google.com/forum/?fromgroups#!topic/cqlengine-users/erkSNe1JwuU

.. module:: cqlengine.connection

The setup function in `cqlengine.connection` records the Cassandra servers to connect to.
If there is a problem with one of the servers, cqlengine will try to connect to each of the other connections before failing.

.. function:: setup(hosts)

    :param hosts: list of hosts, strings in the <hostname>:<port>, or just <hostname>
    :type hosts: list

    :param default_keyspace: keyspace to default to
    :type default_keyspace: str

    :param consistency: the consistency level of the connection, defaults to 'ONE'
    :type consistency: int

    # see http://datastax.github.io/python-driver/api/cassandra.html#cassandra.ConsistencyLevel

    Records the hosts and connects to one of them

See the example at :ref:`getting-started`


