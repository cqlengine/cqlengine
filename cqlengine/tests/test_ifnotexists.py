from cqlengine.management import sync_table, drop_table, create_keyspace, delete_keyspace
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.models import Model
from cqlengine import columns
from uuid import uuid4
import mock
from cqlengine.connection import get_session


class TestIfNotExistsModel(Model):

    __keyspace__ = 'cqlengine_test_lwt'

    id      = columns.UUID(primary_key=True, default=lambda:uuid4())
    count   = columns.Integer()
    text    = columns.Text(required=False)


class BaseIfNotExistsTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseIfNotExistsTest, cls).setUpClass()
        """
        when receiving an insert statement with 'if not exist', cassandra would
        perform a read with QUORUM level. Unittest would be failed if replica_factor
        is 3 and one node only. Therefore I have create a new keyspace with
        replica_factor:1.
        """
        create_keyspace(TestIfNotExistsModel.__keyspace__, replication_factor=1)
        sync_table(TestIfNotExistsModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseCassEngTestCase, cls).tearDownClass()
        drop_table(TestIfNotExistsModel)
        delete_keyspace(TestIfNotExistsModel.__keyspace__)


class IfNotExistsInsertTests(BaseIfNotExistsTest):

    def test_insert_if_not_exists_success(self):
        """ tests that insertion with if_not_exists work as expected """
        id = uuid4()

        TestIfNotExistsModel.create(id=id, count=8, text='123456789')
        TestIfNotExistsModel.if_not_exists(True).create(id=id, count=9, text='111111111111')

        q = TestIfNotExistsModel.objects(id=id)
        self.assertEqual(len(q), 1)

        tm = q.first()
        self.assertEquals(tm.count, 8)
        self.assertEquals(tm.text, '123456789')

    def test_insert_if_not_exists_failure(self):
        """ tests that insertion with if_not_exists failure """
        id = uuid4()

        TestIfNotExistsModel.create(id=id, count=8, text='123456789')
        TestIfNotExistsModel.if_not_exists(False).create(id=id, count=9, text='111111111111')

        q = TestIfNotExistsModel.objects(id=id)
        self.assertEquals(len(q), 1)

        tm = q.first()
        self.assertEquals(tm.count, 9)
        self.assertEquals(tm.text, '111111111111')


class IfNotExistsModelTest(BaseIfNotExistsTest):

    def test_if_not_exists_included_on_create(self):
        """ tests that if_not_exists on models works as expected """

        session = get_session()

        with mock.patch.object(session, 'execute') as m:
            TestIfNotExistsModel.if_not_exists(True).create(count=8)

        query = m.call_args[0][0].query_string
        self.assertIn("IF NOT EXISTS", query)

    def test_if_not_exists_included_on_save(self):

        session = get_session()

        with mock.patch.object(session, 'execute') as m:
            tm = TestIfNotExistsModel(count=8)
            tm.if_not_exists(True).save()

        query = m.call_args[0][0].query_string
        self.assertIn("IF NOT EXISTS", query)

    def test_queryset_is_returned_on_class(self):
        """ ensure we get a queryset description back """
        qs = TestIfNotExistsModel.if_not_exists(True)
        self.assertTrue(isinstance(qs, TestIfNotExistsModel.__queryset__), type(qs))


class IfNotExistsInstanceTest(BaseIfNotExistsTest):

    def test_instance_is_returned(self):
        """
        ensures that we properly handle the instance.if_not_exists(True).save()
        scenario
        """
        o = TestIfNotExistsModel.create(text="whatever")
        o.text = "new stuff"
        o = o.if_not_exists(True)
        self.assertEqual(True, o._if_not_exists)

    def test_if_not_exists_is_not_include_with_query_on_update(self):
        """
        make sure we don't put 'IF NOT EXIST' in update statements
        """
        session = get_session()

        o = TestIfNotExistsModel.create(text="whatever")
        o.text = "new stuff"
        o = o.if_not_exists(True)

        with mock.patch.object(session, 'execute') as m:
            o.save()

        query = m.call_args[0][0].query_string
        self.assertNotIn("IF NOT EXIST", query)


