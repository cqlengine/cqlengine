"""
Test the event feature, including register subscribers and event firing
"""

from uuid import uuid4
from cqlengine.query import BatchQuery
from cqlengine.management import drop_table, sync_table
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.models import Model, columns
from cqlengine.events import add_subscriber, subscriber


class TestEventModel(Model):
    id = columns.UUID(primary_key=True, default=uuid4)
    name = columns.Text()


class TestEvent(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestEvent, cls).setUpClass()
        # drop_table(TestEventModel)
        sync_table(TestEventModel)

    @classmethod
    def tearDownClass(cls):
        super(TestEvent, cls).tearDownClass()
        drop_table(TestEventModel)

    def test_add_subscriber(self):
        @subscriber(TestEventModel.BeforeSave, TestEventModel.AfterSaved)
        def subscriber_A(e): pass
        def subscriber_B(e): pass

        add_subscriber(subscriber_B, TestEventModel.BeforeSave, TestEventModel.BeforeDelete)

        self.assertEqual(len(TestEventModel.BeforeSave._subscribers), 2)
        self.assertEqual(len(TestEventModel.AfterSaved._subscribers), 1)
        self.assertEqual(len(TestEventModel.BeforeDelete._subscribers), 1)
        self.assertIn(subscriber_A, TestEventModel.BeforeSave._subscribers)
        self.assertIn(subscriber_A, TestEventModel.AfterSaved._subscribers)
        self.assertIn(subscriber_B, TestEventModel.BeforeSave._subscribers)
        self.assertIn(subscriber_B, TestEventModel.BeforeDelete._subscribers)


    def test_receive_event(self):
        received_events = []
        def assert_event(instance, event):
            for e in received_events:
                if isinstance(e, event) and e.instance is instance:
                    return
            self.assertTrue(False, 'The expected event "%s" for "%s" did not come'
                                   % (event, instance))

        @subscriber(TestEventModel.BeforeSave,
                    TestEventModel.BeforeUpdate,
                    TestEventModel.BeforeDelete)
        def subscriber_before(e):
            received_events.append(e)

        @subscriber(TestEventModel.AfterSaved,
                    TestEventModel.AfterUpdated,
                    TestEventModel.AfterDeleted)
        def subsriber_after(e):
            if e.__class__ is TestEventModel.AfterSaved:
                before_cls = TestEventModel.BeforeSave
            elif e.__class__ is TestEventModel.AfterUpdated:
                before_cls = TestEventModel.BeforeUpdate
            elif e.__class__ is TestEventModel.AfterDeleted:
                before_cls = TestEventModel.BeforeDelete
            else:
                assert False, 'Unknow "%s" event class' % e.__class__
            assert_event(e.instance, before_cls) # verify that the before event has came
            received_events.append(e)

        t1 = TestEventModel(name='Terk')
        t1.save()

        assert_event(t1, TestEventModel.BeforeSave)
        assert_event(t1, TestEventModel.AfterSaved)

        t2 = TestEventModel(name='Tarzan')
        t2.update(name='Tantor')

        assert_event(t2, TestEventModel.BeforeUpdate)
        assert_event(t2, TestEventModel.AfterUpdated)

        t3 = TestEventModel.objects.filter(TestEventModel.id == t2.id).get()
        t3.delete()

        assert_event(t3, TestEventModel.BeforeDelete)
        assert_event(t3, TestEventModel.AfterDeleted)

        t4 = TestEventModel.create(name='Jane')

        assert_event(t4, TestEventModel.BeforeSave)
        assert_event(t4, TestEventModel.AfterSaved)

    def test_receive_events_in_batch(self):
        received_events = []
        batch = BatchQuery()

        def assert_event(instance, event):
            for e in received_events:
                if isinstance(e, event) \
                        and e.instance is instance\
                        and e.batch is batch:
                    return
            self.assertTrue(False, 'The expected event "%s" for "%s" did not come'
                                   % (event, instance))

        @subscriber(TestEventModel.BeforeSave,
                    TestEventModel.BeforeUpdate,
                    TestEventModel.BeforeDelete)
        def subscriber_before(e):
            received_events.append(e)

        @subscriber(TestEventModel.AfterSaved,
                    TestEventModel.AfterUpdated,
                    TestEventModel.AfterDeleted)
        def subsriber_after(e):
            if e.__class__ is TestEventModel.AfterSaved:
                before_cls = TestEventModel.BeforeSave
            elif e.__class__ is TestEventModel.AfterUpdated:
                before_cls = TestEventModel.BeforeUpdate
            elif e.__class__ is TestEventModel.AfterDeleted:
                before_cls = TestEventModel.BeforeDelete
            else:
                assert False, 'Unknow "%s" event class' % e.__class__
            assert_event(e.instance, before_cls) # verify that the before event has came
            received_events.append(e)

        t1 = TestEventModel(name='Terk')
        t1.batch(batch).save()

        assert_event(t1, TestEventModel.BeforeSave)
        assert_event(t1, TestEventModel.AfterSaved)

        t2 = TestEventModel(name='Tarzan')
        t2.batch(batch).update(name='Tantor')

        assert_event(t2, TestEventModel.BeforeUpdate)
        assert_event(t2, TestEventModel.AfterUpdated)

        t3 = TestEventModel(id=t2.id)
        t3.batch(batch).delete()

        assert_event(t3, TestEventModel.BeforeDelete)
        assert_event(t3, TestEventModel.AfterDeleted)

        t4 = TestEventModel.batch(batch).create(name='Jane')

        assert_event(t4, TestEventModel.BeforeSave)
        assert_event(t4, TestEventModel.AfterSaved)