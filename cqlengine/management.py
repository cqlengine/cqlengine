import json

from cqlengine.connection import connection_manager, execute
from cqlengine.exceptions import CQLEngineException

def create_keyspace(name, strategy_class='SimpleStrategy', replication_factor=3, durable_writes=True, **replication_values):
    """
    creates a keyspace

    :param name: name of keyspace to create
    :param strategy_class: keyspace replication strategy class
    :param replication_factor: keyspace replication factor
    :param durable_writes: 1.2 only, write log is bypassed if set to False
    :param **replication_values: 1.2 only, additional values to ad to the replication data map
    """
    with connection_manager() as con:
        #TODO: check system tables instead of using cql thrifteries
        if not any([name == k.name for k in con.client.describe_keyspaces()]):
    #        if name not in [k.name for k in con.con.client.describe_keyspaces()]:
            try:
                #Try the 1.1 method
                execute("""CREATE KEYSPACE {}
                   WITH strategy_class = '{}'
                   AND strategy_options:replication_factor={};""".format(name, strategy_class, replication_factor))
            except CQLEngineException:
                #try the 1.2 method
                replication_map = {
                    'class': strategy_class,
                    'replication_factor':replication_factor
                }
                replication_map.update(replication_values)

                query = """
                CREATE KEYSPACE {}
                WITH REPLICATION = {}
                """.format(name, json.dumps(replication_map).replace('"', "'"))

                if strategy_class != 'SimpleStrategy':
                    query += " AND DURABLE_WRITES = {}".format('true' if durable_writes else 'false')

                execute(query)

def delete_keyspace(name):
    with connection_manager() as con:
        if name in [k.name for k in con.client.describe_keyspaces()]:
            execute("DROP KEYSPACE {}".format(name))

def create_table(model, create_missing_keyspace=True):
    #construct query string
    cf_name = model.column_family_name()
    raw_cf_name = model.column_family_name(include_keyspace=False)

    #create missing keyspace
    if create_missing_keyspace:
        create_keyspace(model._get_keyspace())

    with connection_manager() as con:
        ks_info = con.client.describe_keyspace(model._get_keyspace())

    #check for an existing column family
    #TODO: check system tables instead of using cql thrifteries
    if not any([raw_cf_name == cf.name for cf in ks_info.cf_defs]):
        qs = ['CREATE TABLE {}'.format(cf_name)]

        #add column types
        pkeys = []
        ckeys = []
        qtypes = []
        def add_column(col):
            s = col.get_column_def()
            if col.primary_key:
                keys = (pkeys if col.partition_key else ckeys)
                keys.append('"{}"'.format(col.db_field_name))
            qtypes.append(s)
        for name, col in model._columns.items():
            add_column(col)

        qtypes.append('PRIMARY KEY (({}){})'.format(', '.join(pkeys), ckeys and ', ' + ', '.join(ckeys) or ''))

        qs += ['({})'.format(', '.join(qtypes))]

        with_qs = ['read_repair_chance = {}'.format(model.read_repair_chance)]

        _order = ["%s %s" % (c.db_field_name, c.clustering_order or 'ASC') for c in model._clustering_keys.values()]
        if _order:
            with_qs.append("clustering order by ({})".format(', '.join(_order)))

        # add read_repair_chance
        qs += ['WITH {}'.format(' AND '.join(with_qs))]
        qs = ' '.join(qs)

        try:
            execute(qs)
        except CQLEngineException as ex:
            # 1.2 doesn't return cf names, so we have to examine the exception
            # and ignore if it says the column family already exists
            if "Cannot add already existing column family" not in unicode(ex):
                raise

    #get existing index names, skip ones that already exist
    with connection_manager() as con:
        ks_info = con.client.describe_keyspace(model._get_keyspace())

    cf_defs = [cf for cf in ks_info.cf_defs if cf.name == raw_cf_name]
    idx_names = [i.index_name for i in  cf_defs[0].column_metadata] if cf_defs else []
    idx_names = filter(None, idx_names)

    indexes = [c for n,c in model._columns.items() if c.index]
    if indexes:
        for column in indexes:
            if column.db_index_name in idx_names: continue
            qs = ['CREATE INDEX index_{}_{}'.format(raw_cf_name, column.db_field_name)]
            qs += ['ON {}'.format(cf_name)]
            qs += ['("{}")'.format(column.db_field_name)]
            qs = ' '.join(qs)

            try:
                execute(qs)
            except CQLEngineException as ex:
                # 1.2 doesn't return cf names, so we have to examine the exception
                # and ignore if it says the index already exists
                if "Index already exists" not in unicode(ex):
                    raise


def delete_table(model):
    cf_name = model.column_family_name()

    try:
        execute('drop table {};'.format(cf_name))
    except CQLEngineException as ex:
        #don't freak out if the table doesn't exist
        if 'Cannot drop non existing column family' not in unicode(ex):
            raise

