from cqlengine.operators import BaseWhereOperator, BaseAssignmentOperator


class StatementException(Exception): pass


class BaseClause(object):

    def __init__(self, field, value):
        self.field = field
        self.value = value
        self.context_id = None

    def __unicode__(self):
        raise NotImplementedError

    def __str__(self):
        return unicode(self).encode('utf-8')

    def get_context_size(self):
        """ returns the number of entries this clause will add to the query context """
        return 1

    def set_context_id(self, i):
        """ sets the value placeholder that will be used in the query """
        self.context_id = i

    def update_context(self, ctx):
        """ updates the query context with this clauses values """
        assert isinstance(ctx, dict)
        ctx[str(self.context_id)] = self.value


class WhereClause(BaseClause):
    """ a single where statement used in queries """

    def __init__(self, field, operator, value):
        if not isinstance(operator, BaseWhereOperator):
            raise StatementException(
                "operator must be of type {}, got {}".format(BaseWhereOperator, type(operator))
            )
        super(WhereClause, self).__init__(field, value)
        self.operator = operator

    def __unicode__(self):
        return u'"{}" {} :{}'.format(self.field, self.operator, self.context_id)


class AssignmentClause(BaseClause):
    """ a single variable st statement """

    def __unicode__(self):
        return u'"{}" = :{}'.format(self.field, self.context_id)

    def insert_tuple(self):
        return self.field, self.context_id


class BaseCQLStatement(object):
    """ The base cql statement class """

    def __init__(self, table, consistency=None, where=None):
        super(BaseCQLStatement, self).__init__()
        self.table = table
        self.consistency = consistency
        self.context_counter = 0

        self.where_clauses = []
        for clause in where or []:
            self.add_where_clause(clause)

    def add_where_clause(self, clause):
        """
        adds a where clause to this statement
        :param clause: the clause to add
        :type clause: WhereClause
        """
        if not isinstance(clause, WhereClause):
            raise StatementException("only instances of WhereClause can be added to statements")
        clause.set_context_id(self.context_counter)
        self.context_counter += clause.get_context_size()
        self.where_clauses.append(clause)

    def get_context(self):
        """
        returns the context dict for this statement
        :rtype: dict
        """
        ctx = {}
        for clause in self.where_clauses or []:
            clause.update_context(ctx)
        return ctx

    def __unicode__(self):
        raise NotImplementedError

    def __str__(self):
        return unicode(self).encode('utf-8')

    @property
    def _where(self):
        return 'WHERE {}'.format(' AND '.join([unicode(c) for c in self.where_clauses]))


class SelectStatement(BaseCQLStatement):
    """ a cql select statement """

    def __init__(self,
                 table,
                 fields=None,
                 count=False,
                 consistency=None,
                 where=None,
                 order_by=None,
                 limit=None,
                 allow_filtering=False):

        super(SelectStatement, self).__init__(
            table,
            consistency=consistency,
            where=where
        )

        self.fields = [fields] if isinstance(fields, basestring) else (fields or [])
        self.count = count
        self.order_by = [order_by] if isinstance(order_by, basestring) else order_by
        self.limit = limit
        self.allow_filtering = allow_filtering

    def __unicode__(self):
        qs = ['SELECT']
        if self.count:
            qs += ['COUNT(*)']
        else:
            qs += [', '.join(['"{}"'.format(f) for f in self.fields]) if self.fields else '*']
        qs += ['FROM', self.table]

        if self.where_clauses:
            qs += [self._where]

        if self.order_by and not self.count:
            qs += ['ORDER BY {}'.format(', '.join(unicode(o) for o in self.order_by))]

        if self.limit and not self.count:
            qs += ['LIMIT {}'.format(self.limit)]

        if self.allow_filtering:
            qs += ['ALLOW FILTERING']

        return ' '.join(qs)


class AssignmentStatement(BaseCQLStatement):
    """ value assignment statements """

    def __init__(self,
                 table,
                 assignments=None,
                 consistency=None,
                 where=None,
                 ttl=None):
        super(AssignmentStatement, self).__init__(
            table,
            consistency=consistency,
            where=where,
        )
        self.ttl = ttl

        # add assignments
        self.assignments = []
        for assignment in assignments or []:
            self.add_assignment_clause(assignment)

    def add_assignment_clause(self, clause):
        """
        adds an assignment clause to this statement
        :param clause: the clause to add
        :type clause: AssignmentClause
        """
        if not isinstance(clause, AssignmentClause):
            raise StatementException("only instances of AssignmentClause can be added to statements")
        clause.set_context_id(self.context_counter)
        self.context_counter += clause.get_context_size()
        self.assignments.append(clause)

    def get_context(self):
        ctx = super(AssignmentStatement, self).get_context()
        for clause in self.assignments:
            clause.update_context(ctx)
        return ctx


class InsertStatement(AssignmentStatement):
    """ an cql insert select statement """

    def add_where_clause(self, clause):
        raise StatementException("Cannot add where clauses to insert statements")

    def __unicode__(self):
        qs = ['INSERT INTO {}'.format(self.table)]

        # get column names and context placeholders
        fields = [a.insert_tuple() for a in self.assignments]
        columns, values = zip(*fields)

        qs += ["({})".format(', '.join(['"{}"'.format(c) for c in columns]))]
        qs += ['VALUES']
        qs += ["({})".format(', '.join([':{}'.format(v) for v in values]))]

        if self.ttl:
            qs += ["USING TTL {}".format(self.ttl)]

        return ' '.join(qs)


class UpdateStatement(AssignmentStatement):
    """ an cql update select statement """

    def __unicode__(self):
        qs = ['UPDATE', self.table]
        qs += ['SET']
        qs += [', '.join([unicode(c) for c in self.assignments])]

        if self.where_clauses:
            qs += [self._where]

        if self.ttl:
            qs += ["USING TTL {}".format(self.ttl)]

        return ' '.join(qs)


class DeleteStatement(BaseCQLStatement):
    """ a cql delete statement """

    def __init__(self, table, fields=None, consistency=None, where=None):
        super(DeleteStatement, self).__init__(
            table,
            consistency=consistency,
            where=where,
        )
        self.fields = [fields] if isinstance(fields, basestring) else (fields or [])

    def __unicode__(self):
        qs = ['DELETE']
        qs += [', '.join(['"{}"'.format(f) for f in self.fields]) if self.fields else '*']
        qs += ['FROM', self.table]

        if self.where_clauses:
            qs += [self._where]

        return ' '.join(qs)
