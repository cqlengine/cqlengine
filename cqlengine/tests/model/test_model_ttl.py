from uuid import uuid4
import random
import time

from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.management import create_table
from cqlengine.management import delete_table
from cqlengine.models import Model
from cqlengine import columns
from cqlengine.exceptions import CQLEngineException

class TestModel(Model):
    id      = columns.UUID(primary_key=True, default=lambda:uuid4())
    text    = columns.Text(required=False)


class TestModelTtl(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestModelTtl, cls).setUpClass()
        create_table(TestModel)

    @classmethod
    def tearDownClass(cls):
        super(TestModelTtl, cls).tearDownClass()
        delete_table(TestModel)

    def test_model_save_with_ttl(self):
        """
        Tests that models can be saved with ttl value
        """
        ttl_value = 800
        tm1 = TestModel.create(text='simple text', ttl=ttl_value)
        tm2 = TestModel(text='simple text 2')
        tm2.save(ttl=ttl_value)

        instances = [tm1, tm2]
        for inst in instances:
            self.assertIsNotNone(inst.id)

    def test_model_get_ttl(self):
        """
        Tests that get ttl value after save with ttl
        """
        ttl_value = 300
        tm = TestModel.objects.create(text='simple text', ttl=ttl_value)

        self.assertLessEqual(tm.get_ttl(), ttl_value)
        self.assertLessEqual(tm.get_ttl('text'), ttl_value)

        def get_ttl_by_undef_column():
            tm.get_ttl('undef_col')

        self.assertRaises(CQLEngineException, get_ttl_by_undef_column)

    def test_model_without_ttl(self):
        """
        Tests that check model save without ttl
        """
        tm = TestModel.objects.create(text='simple text')

        self.assertIsNotNone(tm.id)
        self.assertIsNone(tm.get_ttl())