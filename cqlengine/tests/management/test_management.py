
from cqlengine import ALL, CACHING_ALL, CACHING_NONE
from cqlengine.exceptions import CQLEngineException
from cqlengine.management import  get_fields, sync_table, drop_table
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine import management
from cqlengine.tests.query.test_queryset import TestModel
from cqlengine.models import Model
from cqlengine import columns, SizeTieredCompactionStrategy, LeveledCompactionStrategy



class CreateKeyspaceTest(BaseCassEngTestCase):
    def test_create_succeeeds(self):
        management.create_keyspace('test_keyspace')
        management.delete_keyspace('test_keyspace')

class DeleteTableTest(BaseCassEngTestCase):

    def test_multiple_deletes_dont_fail(self):
        """

        """
        sync_table(TestModel)

        drop_table(TestModel)
        drop_table(TestModel)

class LowercaseKeyModel(Model):
    first_key = columns.Integer(primary_key=True)
    second_key = columns.Integer(primary_key=True)
    some_data = columns.Text()

class CapitalizedKeyModel(Model):
    firstKey = columns.Integer(primary_key=True)
    secondKey = columns.Integer(primary_key=True)
    someData = columns.Text()

class PrimaryKeysOnlyModel(Model):
    __compaction__ = LeveledCompactionStrategy

    first_ey = columns.Integer(primary_key=True)
    second_key = columns.Integer(primary_key=True)


class CapitalizedKeyTest(BaseCassEngTestCase):

    def test_table_definition(self):
        """ Tests that creating a table with capitalized column names succeedso """
        sync_table(LowercaseKeyModel)
        sync_table(CapitalizedKeyModel)

        drop_table(LowercaseKeyModel)
        drop_table(CapitalizedKeyModel)


class FirstModel(Model):
    __table_name__ = 'first_model'
    first_key = columns.UUID(primary_key=True)
    second_key = columns.UUID()
    third_key = columns.Text()

class SecondModel(Model):
    __table_name__ = 'first_model'
    first_key = columns.UUID(primary_key=True)
    second_key = columns.UUID()
    third_key = columns.Text()
    fourth_key = columns.Text()

class ThirdModel(Model):
    __table_name__ = 'first_model'
    first_key = columns.UUID(primary_key=True)
    second_key = columns.UUID()
    third_key = columns.Text()
    # removed fourth key, but it should stay in the DB
    blah = columns.Map(columns.Text, columns.Text)

class FourthModel(Model):
    __table_name__ = 'first_model'
    first_key = columns.UUID(primary_key=True)
    second_key = columns.UUID()
    third_key = columns.Text()
    # removed fourth key, but it should stay in the DB
    renamed = columns.Map(columns.Text, columns.Text, db_field='blah')

class AddColumnTest(BaseCassEngTestCase):
    def setUp(self):
        drop_table(FirstModel)

    def test_add_column(self):
        sync_table(FirstModel)
        fields = get_fields(FirstModel)

        # this should contain the second key
        self.assertEqual(len(fields), 2)
        # get schema
        sync_table(SecondModel)

        fields = get_fields(FirstModel)
        self.assertEqual(len(fields), 3)

        sync_table(ThirdModel)
        fields = get_fields(FirstModel)
        self.assertEqual(len(fields), 4)

        sync_table(FourthModel)
        fields = get_fields(FirstModel)
        self.assertEqual(len(fields), 4)


class ModelWithTableProperties(Model):
    # Set random table properties
    __bloom_filter_fp_chance__ = 0.76328
    __caching__ = CACHING_ALL
    __comment__ = 'TxfguvBdzwROQALmQBOziRMbkqVGFjqcJfVhwGR'
    __default_time_to_live__ = 4756
    __gc_grace_seconds__ = 2063
    __index_interval__ = 98706
    __memtable_flush_period_in_ms__ = 43681
    __populate_io_cache_on_flush__ = True
    __read_repair_chance__ = 0.17985
    __replicate_on_write__ = False
    __dclocal_read_repair_chance__ = 0.50811

    key = columns.UUID(primary_key=True)


class TablePropertiesTests(BaseCassEngTestCase):

    def setUp(self):
        drop_table(ModelWithTableProperties)

    def test_set_table_properties(self):
        sync_table(ModelWithTableProperties)
        self.assertDictContainsSubset({
            'bloom_filter_fp_chance': 0.76328,
            'caching': CACHING_ALL,
            'comment': 'TxfguvBdzwROQALmQBOziRMbkqVGFjqcJfVhwGR',
            'default_time_to_live': 4756,
            'gc_grace_seconds': 2063,
            'index_interval': 98706,
            'memtable_flush_period_in_ms': 43681,
            'populate_io_cache_on_flush': True,
            'read_repair_chance': 0.17985,
            'replicate_on_write': False,
            # For some reason 'dclocal_read_repair_chance' in CQL is called
            # just 'local_read_repair_chance' in the schema table.
            # Source: https://issues.apache.org/jira/browse/CASSANDRA-6717

            # TODO: due to a bug in the native driver i'm not seeing the local read repair chance show up
            #'local_read_repair_chance': 0.50811,

        }, management.get_table_settings(ModelWithTableProperties).options)

    def test_table_property_update(self):
        ModelWithTableProperties.__bloom_filter_fp_chance__ = 0.66778
        ModelWithTableProperties.__caching__ = CACHING_NONE
        ModelWithTableProperties.__comment__ = 'xirAkRWZVVvsmzRvXamiEcQkshkUIDINVJZgLYSdnGHweiBrAiJdLJkVohdRy'
        ModelWithTableProperties.__default_time_to_live__ = 65178
        ModelWithTableProperties.__gc_grace_seconds__ = 96362
        ModelWithTableProperties.__index_interval__ = 94207
        ModelWithTableProperties.__memtable_flush_period_in_ms__ = 60210
        ModelWithTableProperties.__populate_io_cache_on_flush__ = False
        ModelWithTableProperties.__read_repair_chance__ = 0.2989
        ModelWithTableProperties.__replicate_on_write__ = True
        ModelWithTableProperties.__dclocal_read_repair_chance__ = 0.12732

        sync_table(ModelWithTableProperties)

        table_settings = management.get_table_settings(ModelWithTableProperties).options

        self.assertDictContainsSubset({
            'bloom_filter_fp_chance': 0.66778,
            'caching': CACHING_NONE,
            'comment': 'xirAkRWZVVvsmzRvXamiEcQkshkUIDINVJZgLYSdnGHweiBrAiJdLJkVohdRy',
            'default_time_to_live': 65178,
            'gc_grace_seconds': 96362,
            'index_interval': 94207,
            'memtable_flush_period_in_ms': 60210,
            'populate_io_cache_on_flush': False,
            'read_repair_chance': 0.2989,
            'replicate_on_write': True,

            # TODO see above comment re: native driver missing local read repair chance
            # 'local_read_repair_chance': 0.12732,
        }, table_settings)


class SyncTableTests(BaseCassEngTestCase):

    def setUp(self):
        drop_table(PrimaryKeysOnlyModel)

    def test_sync_table_works_with_primary_keys_only_tables(self):

        # This is "create table":

        sync_table(PrimaryKeysOnlyModel)

        # let's make sure settings persisted correctly:

        assert PrimaryKeysOnlyModel.__compaction__ == LeveledCompactionStrategy
        # blows up with DoesNotExist if table does not exist
        table_settings = management.get_table_settings(PrimaryKeysOnlyModel)
        # let make sure the flag we care about

        assert LeveledCompactionStrategy in table_settings.options['compaction_strategy_class']


        # Now we are "updating" the table:

        # setting up something to change
        PrimaryKeysOnlyModel.__compaction__ = SizeTieredCompactionStrategy

        # primary-keys-only tables do not create entries in system.schema_columns
        # table. Only non-primary keys are added to that table.
        # Our code must deal with that eventuality properly (not crash)
        # on subsequent runs of sync_table (which runs get_fields internally)
        get_fields(PrimaryKeysOnlyModel)
        sync_table(PrimaryKeysOnlyModel)

        table_settings = management.get_table_settings(PrimaryKeysOnlyModel)
        assert SizeTieredCompactionStrategy in table_settings.options['compaction_strategy_class']

class NonModelFailureTest(BaseCassEngTestCase):
    class FakeModel(object):
        pass

    def test_failure(self):
        with self.assertRaises(CQLEngineException):
            sync_table(self.FakeModel)
