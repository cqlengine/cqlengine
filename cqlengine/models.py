from collections import OrderedDict
import re

from cqlengine import columns
from cqlengine.exceptions import ModelException
from cqlengine.functions import BaseQueryFunction
from cqlengine.query import QuerySet, QueryException, DMLQuery

class ModelDefinitionException(ModelException): pass

DEFAULT_KEYSPACE = 'cqlengine'

class hybrid_classmethod(object):
    """
    Allows a method to behave as both a class method and
    normal instance method depending on how it's called
    """

    def __init__(self, clsmethod, instmethod):
        self.clsmethod = clsmethod
        self.instmethod = instmethod

    def __get__(self, instance, owner):
        if instance is None:
            return self.clsmethod.__get__(owner, owner)
        else:
            return self.instmethod.__get__(instance, owner)

class BaseModel(object):
    """
    The base model class, don't inherit from this, inherit from Model, defined below
    """
    
    class DoesNotExist(QueryException): pass
    class MultipleObjectsReturned(QueryException): pass

    #table names will be generated automatically from it's model and package name
    #however, you can also define them manually here
    table_name = None

    #the keyspace for this model 
    keyspace = None
    read_repair_chance = 0.1

    def __init__(self, **values):
        self._values = {}
        for name, column in self._columns.items():
            value =  values.get(name, None)
            if value is not None: value = column.to_python(value)
            value_mngr = column.value_manager(self, column, value)
            self._values[name] = value_mngr

        # a flag set by the deserializer to indicate
        # that update should be used when persisting changes
        self._is_persisted = False
        self._batch = None

    def _can_update(self):
        """
        Called by the save function to check if this should be
        persisted with update or insert

        :return:
        """
        if not self._is_persisted: return False
        pks = self._primary_keys.keys()
        return all([not self._values[k].changed for k in self._primary_keys])

    @classmethod
    def _get_keyspace(cls):
        """ Returns the manual keyspace, if set, otherwise the default keyspace """
        return cls.keyspace or DEFAULT_KEYSPACE

    def __eq__(self, other):
        return self.as_dict() == other.as_dict()

    def __ne__(self, other):
        return not self.__eq__(other)

    @classmethod
    def column_family_name(cls, include_keyspace=True):
        """
        Returns the column family name if it's been defined
        otherwise, it creates it from the module and class name
        """
        cf_name = ''
        if cls.table_name:
            cf_name = cls.table_name.lower()
        else:
            camelcase = re.compile(r'([a-z])([A-Z])')
            ccase = lambda s: camelcase.sub(lambda v: '{}_{}'.format(v.group(1), v.group(2).lower()), s)
    
            module = cls.__module__.split('.')
            if module:
                cf_name = ccase(module[-1]) + '_'
    
            cf_name += ccase(cls.__name__)
            #trim to less than 48 characters or cassandra will complain
            cf_name = cf_name[-48:]
            cf_name = cf_name.lower()
            cf_name = re.sub(r'^_+', '', cf_name)
        if not include_keyspace: return cf_name
        return '{}.{}'.format(cls._get_keyspace(), cf_name)

    @property
    def pk(self):
        """ Returns the object's primary key """
        return getattr(self, self._pk_name)

    def validate(self):
        """ Cleans and validates the field values """
        for name, col in self._columns.items():
            val = col.validate(getattr(self, name))
            setattr(self, name, val)

    def as_dict(self):
        """ Returns a map of column names to cleaned values """
        values = self._dynamic_columns or {}
        for name, col in self._columns.items():
            values[name] = col.to_database(getattr(self, name, None))
        return values

    @classmethod
    def create(cls, **kwargs):
        return cls.objects.create(**kwargs)
    
    @classmethod
    def all(cls):
        return cls.objects.all()
    
    @classmethod
    def filter(cls, **kwargs):
        return cls.objects.filter(**kwargs)
    
    @classmethod
    def get(cls, **kwargs):
        return cls.objects.get(**kwargs)

    def save(self):
        is_new = self.pk is None
        self.validate()
        DMLQuery(self.__class__, self, batch=self._batch).save()

        #reset the value managers
        for v in self._values.values():
            v.reset_previous_value()
        self._is_persisted = True

        return self

    def delete(self):
        """ Deletes this instance """
        DMLQuery(self.__class__, self, batch=self._batch).delete()

    @classmethod
    def _class_batch(cls, batch):
        return cls.objects.batch(batch)

    def _inst_batch(self, batch):
        self._batch = batch
        return self

    batch = hybrid_classmethod(_class_batch, _inst_batch)



class ModelMetaClass(type):

    def __new__(cls, name, bases, attrs):
        """
        """
        #move column definitions into columns dict
        #and set default column names
        column_dict = OrderedDict()
        primary_keys = OrderedDict()
        pk_name = None
        primary_key = None

        #get inherited properties
        inherited_columns = OrderedDict()
        for base in bases:
            for k,v in getattr(base, '_defined_columns', {}).items():
                inherited_columns.setdefault(k,v)

        def _transform_column(col_name, col_obj):
            column_dict[col_name] = col_obj
            if col_obj.primary_key:
                primary_keys[col_name] = col_obj
            col_obj.set_column_name(col_name)
            #set properties
            _get = lambda self: self._values[col_name].getval()
            _set = lambda self, val: self._values[col_name].setval(val)
            _del = lambda self: self._values[col_name].delval()
            if col_obj.can_delete:
                attrs[col_name] = property(_get, _set)
            else:
                attrs[col_name] = property(_get, _set, _del)

        column_definitions = [(k,v) for k,v in attrs.items() if isinstance(v, columns.Column)]
        column_definitions = sorted(column_definitions, lambda x,y: cmp(x[1].position, y[1].position))

        column_definitions = inherited_columns.items() + column_definitions

        #columns defined on model, excludes automatically
        #defined columns
        defined_columns = OrderedDict(column_definitions)

        #prepend primary key if one hasn't been defined
        if not any([v.primary_key for k,v in column_definitions]):
            k,v = 'id', columns.UUID(primary_key=True)
            column_definitions = [(k,v)] + column_definitions

        #TODO: check that the defined columns don't conflict with any of the Model API's existing attributes/methods
        #transform column definitions
        for k,v in column_definitions:
            if pk_name is None and v.primary_key:
                pk_name = k
                primary_key = v
                v._partition_key = True
            _transform_column(k,v)
        
        #setup primary key shortcut
        if pk_name != 'pk':
            attrs['pk'] = attrs[pk_name]

        #check for duplicate column names
        col_names = set()
        for v in column_dict.values():
            if v.db_field_name in col_names:
                raise ModelException("{} defines the column {} more than once".format(name, v.db_field_name))
            col_names.add(v.db_field_name)

        #create db_name -> model name map for loading
        db_map = {}
        for field_name, col in column_dict.items():
            db_map[col.db_field_name] = field_name

        #short circuit table_name inheritance
        attrs['table_name'] = attrs.get('table_name')

        #add management members to the class
        attrs['_columns'] = column_dict
        attrs['_primary_keys'] = primary_keys
        attrs['_defined_columns'] = defined_columns
        attrs['_db_map'] = db_map
        attrs['_pk_name'] = pk_name
        attrs['_primary_key'] = primary_key
        attrs['_dynamic_columns'] = {}

        #create the class and add a QuerySet to it
        klass = super(ModelMetaClass, cls).__new__(cls, name, bases, attrs)
        klass.objects = QuerySet(klass)
        return klass


class Model(BaseModel):
    """
    the db name for the column family can be set as the attribute db_name, or
    it will be genertaed from the class name
    """
    __metaclass__ = ModelMetaClass


