import copy
from datetime import datetime
from cqlengine import BaseContainerColumn, Map, columns
from cqlengine.columns import Counter, List, Set

from cqlengine.connection import execute, RowResult

from cqlengine.exceptions import CQLEngineException, ValidationError
from cqlengine.functions import Token, BaseQueryFunction, QueryValue

#CQL 3 reference:
#http://www.datastax.com/docs/1.1/references/cql/index
from cqlengine.operators import InOperator, EqualsOperator, GreaterThanOperator, GreaterThanOrEqualOperator
from cqlengine.operators import LessThanOperator, LessThanOrEqualOperator, BaseWhereOperator
from cqlengine.statements import WhereClause, SelectStatement, DeleteStatement, UpdateStatement, AssignmentClause, InsertStatement, BaseCQLStatement, MapUpdateClause, MapDeleteClause, ListUpdateClause, SetUpdateClause, CounterUpdateClause


class QueryException(CQLEngineException): pass
class DoesNotExist(QueryException): pass
class MultipleObjectsReturned(QueryException): pass


class AbstractQueryableColumn(object):
    """
    exposes cql query operators through pythons
    builtin comparator symbols
    """

    def _get_column(self):
        raise NotImplementedError

    def __unicode__(self):
        raise NotImplementedError

    def __str__(self):
        return str(unicode(self))

    def _to_database(self, val):
        if isinstance(val, QueryValue):
            return val
        else:
            return self._get_column().to_database(val)

    def in_(self, item):
        """
        Returns an in operator

        used where you'd typically want to use python's `in` operator
        """
        return WhereClause(unicode(self), InOperator(), item)

    def __eq__(self, other):
        return WhereClause(unicode(self), EqualsOperator(), self._to_database(other))

    def __gt__(self, other):
        return WhereClause(unicode(self), GreaterThanOperator(), self._to_database(other))

    def __ge__(self, other):
        return WhereClause(unicode(self), GreaterThanOrEqualOperator(), self._to_database(other))

    def __lt__(self, other):
        return WhereClause(unicode(self), LessThanOperator(), self._to_database(other))

    def __le__(self, other):
        return WhereClause(unicode(self), LessThanOrEqualOperator(), self._to_database(other))


class BatchType(object):
    Unlogged    = 'UNLOGGED'
    Counter     = 'COUNTER'


class BatchQuery(object):
    """
    Handles the batching of queries

    http://www.datastax.com/docs/1.2/cql_cli/cql/BATCH
    """
    _consistency = None


    def __init__(self, batch_type=None, timestamp=None, consistency=None, execute_on_exception=False):
        self.queries = []
        self.batch_type = batch_type
        if timestamp is not None and not isinstance(timestamp, datetime):
            raise CQLEngineException('timestamp object must be an instance of datetime')
        self.timestamp = timestamp
        self._consistency = consistency
        self._execute_on_exception = execute_on_exception

    def add_query(self, query):
        if not isinstance(query, BaseCQLStatement):
            raise CQLEngineException('only BaseCQLStatements can be added to a batch query')
        self.queries.append(query)

    def consistency(self, consistency):
        self._consistency = consistency

    def execute(self):
        if len(self.queries) == 0:
            # Empty batch is a no-op
            return

        opener = 'BEGIN ' + (self.batch_type + ' ' if self.batch_type else '') + ' BATCH'
        if self.timestamp:
            epoch = datetime(1970, 1, 1)
            ts = long((self.timestamp - epoch).total_seconds() * 1000)
            opener += ' USING TIMESTAMP {}'.format(ts)

        query_list = [opener]
        parameters = {}
        ctx_counter = 0
        for query in self.queries:
            query.update_context_id(ctx_counter)
            ctx = query.get_context()
            ctx_counter += len(ctx)
            query_list.append('  ' + str(query))
            parameters.update(ctx)

        query_list.append('APPLY BATCH;')

        execute('\n'.join(query_list), parameters, self._consistency)

        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        #don't execute if there was an exception by default
        if exc_type is not None and not self._execute_on_exception: return
        self.execute()


class AbstractQuerySet(object):

    def __init__(self, model):
        super(AbstractQuerySet, self).__init__()
        self.model = model

        #Where clause filters
        self._where = []

        #ordering arguments
        self._order = []

        self._allow_filtering = False

        #CQL has a default limit of 10000, it's defined here
        #because explicit is better than implicit
        self._limit = 10000

        #see the defer and only methods
        self._defer_fields = []
        self._only_fields = []

        self._values_list = False
        self._flat_values_list = False

        #results cache
        self._con = None
        self._cur = None
        self._result_cache = None
        self._result_idx = None

        self._batch = None
        self._ttl = None
        self._consistency = None

    @property
    def column_family_name(self):
        return self.model.column_family_name()

    def _execute(self, q):
        if self._batch:
            return self._batch.add_query(q)
        else:
            return execute(q, consistency_level=self._consistency)

    def __unicode__(self):
        return unicode(self._select_query())

    def __str__(self):
        return str(self.__unicode__())

    def __call__(self, *args, **kwargs):
        return self.filter(*args, **kwargs)

    def __deepcopy__(self, memo):
        clone = self.__class__(self.model)
        for k, v in self.__dict__.items():
            if k in ['_con', '_cur', '_result_cache', '_result_idx']: # don't clone these
                clone.__dict__[k] = None
            elif k == '_batch':
                # we need to keep the same batch instance across
                # all queryset clones, otherwise the batched queries
                # fly off into other batch instances which are never
                # executed, thx @dokai
                clone.__dict__[k] = self._batch
            else:
                clone.__dict__[k] = copy.deepcopy(v, memo)

        return clone

    def __len__(self):
        self._execute_query()
        return len(self._result_cache)

    #----query generation / execution----

    def _select_fields(self):
        """ returns the fields to select """
        return []

    def _validate_select_where(self):
        """ put select query validation here """

    def _select_query(self):
        """
        Returns a select clause based on the given filter args
        """
        if self._where:
            self._validate_select_where()
        return SelectStatement(
            self.column_family_name,
            fields=self._select_fields(),
            where=self._where,
            order_by=self._order,
            limit=self._limit,
            allow_filtering=self._allow_filtering
        )

    #----Reads------

    def _execute_query(self):
        if self._batch:
            raise CQLEngineException("Only inserts, updates, and deletes are available in batch mode")
        if self._result_cache is None:
            columns, self._result_cache = self._execute(self._select_query())
            self._construct_result = self._get_result_constructor(columns)

    def _fill_result_cache_to_idx(self, idx):
        self._execute_query()
        if self._result_idx is None:
            self._result_idx = -1

        qty = idx - self._result_idx
        if qty < 1:
            return
        else:
            for idx in range(qty):
                self._result_idx += 1
                self._result_cache[self._result_idx] = self._construct_result(self._result_cache[self._result_idx])

            #return the connection to the connection pool if we have all objects
            if self._result_cache and self._result_idx == (len(self._result_cache) - 1):
                self._con = None
                self._cur = None

    def __iter__(self):
        self._execute_query()

        for idx in range(len(self._result_cache)):
            instance = self._result_cache[idx]
            if isinstance(instance, RowResult):
                self._fill_result_cache_to_idx(idx)
            yield self._result_cache[idx]

    def __getitem__(self, s):
        self._execute_query()

        num_results = len(self._result_cache)

        if isinstance(s, slice):
            #calculate the amount of results that need to be loaded
            end = num_results if s.step is None else s.step
            if end < 0:
                end += num_results
            else:
                end -= 1
            self._fill_result_cache_to_idx(end)
            return self._result_cache[s.start:s.stop:s.step]
        else:
            #return the object at this index
            s = long(s)

            #handle negative indexing
            if s < 0: s += num_results

            if s >= num_results:
                raise IndexError
            else:
                self._fill_result_cache_to_idx(s)
                return self._result_cache[s]

    def _get_result_constructor(self, names):
        """
        Returns a function that will be used to instantiate query results
        """
        raise NotImplementedError

    def batch(self, batch_obj):
        """
        Adds a batch query to the mix
        :param batch_obj:
        :return:
        """
        if batch_obj is not None and not isinstance(batch_obj, BatchQuery):
            raise CQLEngineException('batch_obj must be a BatchQuery instance or None')
        clone = copy.deepcopy(self)
        clone._batch = batch_obj
        return clone

    def first(self):
        try:
            return iter(self).next()
        except StopIteration:
            return None

    def all(self):
        return copy.deepcopy(self)

    def consistency(self, consistency):
        clone = copy.deepcopy(self)
        clone._consistency = consistency
        return clone

    def _parse_filter_arg(self, arg):
        """
        Parses a filter arg in the format:
        <colname>__<op>
        :returns: colname, op tuple
        """
        statement = arg.rsplit('__', 1)
        if len(statement) == 1:
            return arg, None
        elif len(statement) == 2:
            return statement[0], statement[1]
        else:
            raise QueryException("Can't parse '{}'".format(arg))

    def filter(self, *args, **kwargs):
        """
        Adds WHERE arguments to the queryset, returning a new queryset

        #TODO: show examples

        :rtype: AbstractQuerySet
        """
        #add arguments to the where clause filters
        clone = copy.deepcopy(self)
        for operator in args:
            if not isinstance(operator, WhereClause):
                raise QueryException('{} is not a valid query operator'.format(operator))
            clone._where.append(operator)

        for arg, val in kwargs.items():
            col_name, col_op = self._parse_filter_arg(arg)
            quote_field = True
            #resolve column and operator
            try:
                column = self.model._get_column(col_name)
            except KeyError:
                if col_name == 'pk__token':
                    if not isinstance(val, Token):
                        raise QueryException("Virtual column 'pk__token' may only be compared to Token() values")
                    column = columns._PartitionKeysToken(self.model)
                    quote_field = False
                else:
                    raise QueryException("Can't resolve column name: '{}'".format(col_name))

            if isinstance(val, Token):
                if col_name != 'pk__token':
                    raise QueryException("Token() values may only be compared to the 'pk__token' virtual column")
                partition_columns = column.partition_columns
                if len(partition_columns) != len(val.value):
                    raise QueryException(
                        'Token() received {} arguments but model has {} partition keys'.format(
                            len(partition_columns), len(val.value)))
                val.set_columns(partition_columns)

            #get query operator, or use equals if not supplied
            operator_class = BaseWhereOperator.get_operator(col_op or 'EQ')
            operator = operator_class()

            if isinstance(operator, InOperator):
                if not isinstance(val, (list, tuple)):
                    raise QueryException('IN queries must use a list/tuple value')
                query_val = [column.to_database(v) for v in val]
            elif isinstance(val, BaseQueryFunction):
                query_val = val
            else:
                query_val = column.to_database(val)

            clone._where.append(WhereClause(column.db_field_name, operator, query_val, quote_field=quote_field))

        return clone

    def get(self, *args, **kwargs):
        """
        Returns a single instance matching this query, optionally with additional filter kwargs.

        A DoesNotExistError will be raised if there are no rows matching the query
        A MultipleObjectsFoundError will be raised if there is more than one row matching the queyr
        """
        if args or kwargs:
            return self.filter(*args, **kwargs).get()

        self._execute_query()
        if len(self._result_cache) == 0:
            raise self.model.DoesNotExist
        elif len(self._result_cache) > 1:
            raise self.model.MultipleObjectsReturned(
                    '{} objects found'.format(len(self._result_cache)))
        else:
            return self[0]

    def _get_ordering_condition(self, colname):
        order_type = 'DESC' if colname.startswith('-') else 'ASC'
        colname = colname.replace('-', '')

        return colname, order_type

    def order_by(self, *colnames):
        """
        orders the result set.
        ordering can only use clustering columns.

        Default order is ascending, prepend a '-' to the column name for descending
        """
        if len(colnames) == 0:
            clone = copy.deepcopy(self)
            clone._order = []
            return clone

        conditions = []
        for colname in colnames:
            conditions.append('"{}" {}'.format(*self._get_ordering_condition(colname)))

        clone = copy.deepcopy(self)
        clone._order.extend(conditions)
        return clone

    def count(self):
        """ Returns the number of rows matched by this query """
        if self._batch:
            raise CQLEngineException("Only inserts, updates, and deletes are available in batch mode")

        if self._result_cache is None:
            query = self._select_query()
            query.count = True
            _, result = self._execute(query)
            return result[0][0]
        else:
            return len(self._result_cache)

    def limit(self, v):
        """
        Sets the limit on the number of results returned
        CQL has a default limit of 10,000
        """
        if not (v is None or isinstance(v, (int, long))):
            raise TypeError
        if v == self._limit:
            return self

        if v < 0:
            raise QueryException("Negative limit is not allowed")

        clone = copy.deepcopy(self)
        clone._limit = v
        return clone

    def allow_filtering(self):
        """
        Enables the unwise practive of querying on a clustering
        key without also defining a partition key
        """
        clone = copy.deepcopy(self)
        clone._allow_filtering = True
        return clone

    def _only_or_defer(self, action, fields):
        clone = copy.deepcopy(self)
        if clone._defer_fields or clone._only_fields:
            raise QueryException("QuerySet alread has only or defer fields defined")

        #check for strange fields
        missing_fields = [f for f in fields if f not in self.model._columns.keys()]
        if missing_fields:
            raise QueryException(
                "Can't resolve fields {} in {}".format(
                    ', '.join(missing_fields), self.model.__name__))

        if action == 'defer':
            clone._defer_fields = fields
        elif action == 'only':
            clone._only_fields = fields
        else:
            raise ValueError

        return clone

    def only(self, fields):
        """ Load only these fields for the returned query """
        return self._only_or_defer('only', fields)

    def defer(self, fields):
        """ Don't load these fields for the returned query """
        return self._only_or_defer('defer', fields)

    def create(self, **kwargs):
        return self.model(**kwargs).batch(self._batch).ttl(self._ttl).consistency(self._consistency).save()

    def delete(self):
        """
        Deletes the contents of a query
        """
        #validate where clause
        partition_key = self.model._primary_keys.values()[0]
        if not any([c.field == partition_key.column_name for c in self._where]):
            raise QueryException("The partition key must be defined on delete queries")

        dq = DeleteStatement(
            self.column_family_name,
            where=self._where
        )
        self._execute(dq)

    def __eq__(self, q):
        if len(self._where) == len(q._where):
            return all([w in q._where for w in self._where])
        return False

    def __ne__(self, q):
        return not (self != q)


class ResultObject(dict):
    """
    adds attribute access to a dictionary
    """

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError


class SimpleQuerySet(AbstractQuerySet):
    """

    """

    def _get_result_constructor(self, names):
        """
        Returns a function that will be used to instantiate query results
        """
        def _construct_instance(values):
            return ResultObject(zip(names, values))
        return _construct_instance


class ModelQuerySet(AbstractQuerySet):
    """

    """
    def _validate_select_where(self):
        """ Checks that a filterset will not create invalid select statement """
        #check that there's either a = or IN relationship with a primary key or indexed field
        equal_ops = [self.model._columns.get(w.field) for w in self._where if isinstance(w.operator, EqualsOperator)]
        token_comparison = any([w for w in self._where if isinstance(w.value, Token)])
        if not any([w.primary_key or w.index for w in equal_ops]) and not token_comparison:
            raise QueryException('Where clauses require either a "=" or "IN" comparison with either a primary key or indexed field')

        if not self._allow_filtering:
            #if the query is not on an indexed field
            if not any([w.index for w in equal_ops]):
                if not any([w.partition_key for w in equal_ops]) and not token_comparison:
                    raise QueryException('Filtering on a clustering key without a partition key is not allowed unless allow_filtering() is called on the querset')

    def _select_fields(self):
        if self._defer_fields or self._only_fields:
            fields = self.model._columns.keys()
            if self._defer_fields:
                fields = [f for f in fields if f not in self._defer_fields]
            elif self._only_fields:
                fields = self._only_fields
            return [self.model._columns[f].db_field_name for f in fields]
        return super(ModelQuerySet, self)._select_fields()

    def _get_result_constructor(self, names):
        """ Returns a function that will be used to instantiate query results """
        if not self._values_list:
            return lambda values: self.model._construct_instance(names, values)
        else:
            columns = [self.model._columns[n] for n in names]
            if self._flat_values_list:
                return lambda values: columns[0].to_python(values[0])
            else:
                return lambda values: map(lambda (c, v): c.to_python(v), zip(columns, values))

    def _get_ordering_condition(self, colname):
        colname, order_type = super(ModelQuerySet, self)._get_ordering_condition(colname)

        column = self.model._columns.get(colname)
        if column is None:
            raise QueryException("Can't resolve the column name: '{}'".format(colname))

        #validate the column selection
        if not column.primary_key:
            raise QueryException(
                "Can't order on '{}', can only order on (clustered) primary keys".format(colname))

        pks = [v for k, v in self.model._columns.items() if v.primary_key]
        if column == pks[0]:
            raise QueryException(
                "Can't order by the first primary key (partition key), clustering (secondary) keys only")

        return column.db_field_name, order_type

    def values_list(self, *fields, **kwargs):
        """ Instructs the query set to return tuples, not model instance """
        flat = kwargs.pop('flat', False)
        if kwargs:
            raise TypeError('Unexpected keyword arguments to values_list: %s'
                            % (kwargs.keys(),))
        if flat and len(fields) > 1:
            raise TypeError("'flat' is not valid when values_list is called with more than one field.")
        clone = self.only(fields)
        clone._values_list = True
        clone._flat_values_list = flat
        return clone

    def ttl(self, ttl):
        clone = copy.deepcopy(self)
        clone._ttl = ttl
        return clone

    def update(self, **values):
        """ Updates the rows in this queryset """
        if not values:
            return

        nulled_columns = set()
        us = UpdateStatement(self.column_family_name, where=self._where, ttl=self._ttl)
        for name, val in values.items():
            col = self.model._columns.get(name)
            # check for nonexistant columns
            if col is None:
                raise ValidationError("{}.{} has no column named: {}".format(self.__module__, self.model.__name__, name))
            # check for primary key update attempts
            if col.is_primary_key:
                raise ValidationError("Cannot apply update to primary key '{}' for {}.{}".format(name, self.__module__, self.model.__name__))

            val = col.validate(val)
            if val is None:
                nulled_columns.add(name)
                continue
            # add the update statements
            if isinstance(col, Counter):
                # TODO: implement counter updates
                raise NotImplementedError
            else:
                us.add_assignment_clause(AssignmentClause(name, col.to_database(val)))

        if us.assignments:
            self._execute(us)

        if nulled_columns:
            ds = DeleteStatement(self.column_family_name, fields=nulled_columns, where=self._where)
            self._execute(ds)


class DMLQuery(object):
    """
    A query object used for queries performing inserts, updates, or deletes

    this is usually instantiated by the model instance to be modified

    unlike the read query object, this is mutable
    """
    _ttl = None
    _consistency = None

    def __init__(self, model, instance=None, batch=None, ttl=None, consistency=None):
        self.model = model
        self.column_family_name = self.model.column_family_name()
        self.instance = instance
        self._batch = batch
        self._ttl = ttl
        self._consistency = consistency

    def _execute(self, q):
        if self._batch:
            return self._batch.add_query(q)
        else:
            return execute(q, consistency_level=self._consistency)

    def batch(self, batch_obj):
        if batch_obj is not None and not isinstance(batch_obj, BatchQuery):
            raise CQLEngineException('batch_obj must be a BatchQuery instance or None')
        self._batch = batch_obj
        return self

    def _delete_null_columns(self):
        """
        executes a delete query to remove columns that have changed to null
        """
        ds = DeleteStatement(self.column_family_name)
        deleted_fields = False
        for _, v in self.instance._values.items():
            col = v.column
            if v.deleted:
                ds.add_field(col.db_field_name)
                deleted_fields = True
            elif isinstance(col, Map):
                uc = MapDeleteClause(col.db_field_name, v.value, v.previous_value)
                if uc.get_context_size() > 0:
                    ds.add_field(uc)
                    deleted_fields = True

        if deleted_fields:
            for name, col in self.model._primary_keys.items():
                ds.add_where_clause(WhereClause(
                    col.db_field_name,
                    EqualsOperator(),
                    col.to_database(getattr(self.instance, name))
                ))
            self._execute(ds)

    def update(self):
        """
        updates a row.
        This is a blind update call.
        All validation and cleaning needs to happen
        prior to calling this.
        """
        if self.instance is None:
            raise CQLEngineException("DML Query intance attribute is None")
        assert type(self.instance) == self.model

        statement = UpdateStatement(self.column_family_name, ttl=self._ttl)
        #get defined fields and their column names
        for name, col in self.model._columns.items():
            if not col.is_primary_key:
                val = getattr(self.instance, name, None)
                val_mgr = self.instance._values[name]

                # don't update something that is null
                if val is None:
                    continue

                # don't update something if it hasn't changed
                if not val_mgr.changed and not isinstance(col, Counter):
                    continue

                if isinstance(col, (BaseContainerColumn, Counter)):
                    # get appropriate clause
                    if isinstance(col, List): klass = ListUpdateClause
                    elif isinstance(col, Map): klass = MapUpdateClause
                    elif isinstance(col, Set): klass = SetUpdateClause
                    elif isinstance(col, Counter): klass = CounterUpdateClause
                    else: raise RuntimeError

                    # do the stuff
                    clause = klass(col.db_field_name, val, val_mgr.previous_value, column=col)
                    if clause.get_context_size() > 0:
                        statement.add_assignment_clause(clause)
                else:
                    statement.add_assignment_clause(AssignmentClause(
                        col.db_field_name,
                        col.to_database(val)
                    ))

        if statement.get_context_size() > 0 or self.instance._has_counter:
            for name, col in self.model._primary_keys.items():
                statement.add_where_clause(WhereClause(
                    col.db_field_name,
                    EqualsOperator(),
                    col.to_database(getattr(self.instance, name))
                ))
            self._execute(statement)

        self._delete_null_columns()

    def save(self):
        """
        Creates / updates a row.
        This is a blind insert call.
        All validation and cleaning needs to happen
        prior to calling this.
        """
        if self.instance is None:
            raise CQLEngineException("DML Query intance attribute is None")
        assert type(self.instance) == self.model

        nulled_fields = set()
        if self.instance._has_counter or self.instance._can_update():
            return self.update()
        else:
            insert = InsertStatement(self.column_family_name, ttl=self._ttl)
            for name, col in self.instance._columns.items():
                val = getattr(self.instance, name, None)
                if col._val_is_null(val):
                    if self.instance._values[name].changed:
                        nulled_fields.add(col.db_field_name)
                    continue
                insert.add_assignment_clause(AssignmentClause(
                    col.db_field_name,
                    col.to_database(getattr(self.instance, name, None))
                ))

        # skip query execution if it's empty
        # caused by pointless update queries
        if not insert.is_empty:
            self._execute(insert)

        # delete any nulled columns
        self._delete_null_columns()

    def delete(self):
        """ Deletes one instance """
        if self.instance is None:
            raise CQLEngineException("DML Query instance attribute is None")

        ds = DeleteStatement(self.column_family_name)
        for name, col in self.model._primary_keys.items():
            ds.add_where_clause(WhereClause(
                col.db_field_name,
                EqualsOperator(),
                col.to_database(getattr(self.instance, name))
            ))
        self._execute(ds)


