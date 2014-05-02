"""
Allow the sources code to subscribe event. The currently available events are:
1. BeforeSave
2. AfterSaved
3. BeforeUpdate
4. AfterUpdated
3. BeforeDelete
4. AfterDeleted
"""

import collections


class EventSubscribers(collections.MutableSet):
    """
    Manage all subscribers for an event class
    Notify an event to all its subscribers

    This class also simulates the OrderedSet as recommendation from
      https://docs.python.org/2/library/collections.html
      http://code.activestate.com/recipes/576694/
    """

    def __init__(self, event_class):
        self._event_class = event_class
        self.end = []
        self.end += [None, self.end, self.end]   # sentinel node for doubly linked list
        self.map = {}                           # key --> [key, prev, next]

    def add(self, key):
        if key not in self.map:
            end = self.end
            curr = end[1]
            curr[2] = end[1] = self.map[key] = [key, curr, end]

    def discard(self, key):
        if key in self.map:
            key, prev, next = self.map.pop(key)
            prev[2] = next
            next[1] = prev

    def __iter__(self):
        end = self.end
        curr = end[2]
        while curr is not end:
            yield curr[0]
            curr = curr[2]

    def __reversed__(self):
        end = self.end
        curr = end[1]
        while curr is not end:
            yield curr[0]
            curr = curr[1]

    def pop(self, last=True):
        if not self:
            raise KeyError('set is empty')
        key = self.end[1][0] if last else self.end[2][0]
        self.discard(key)
        return key

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, EventSubscribers):
            return len(self) == len(other) and list(self) == list(other)
        return set(self) == set(other)

    def __len__(self):
        return len(self.map)

    def __contains__(self, key):
        return key in self.map

    def notify(self, event):
        assert type(event) == self._event_class
        for fn in self:
            fn(event)


class ModelEventMetaClass(type):
    def __new__(mcs, name, bases, attrs):
        """
        Create the class and add a dispatcher to it
        """
        klass = super(ModelEventMetaClass, mcs).__new__(mcs, name, bases, attrs)
        klass._subscribers = EventSubscribers(klass)
        return klass


class ModelEvent(object):
    """
    Base class for all model's events
    """
    __metaclass__ = ModelEventMetaClass

    def __init__(self, dmlquery):
        """
        create new event instance from a dmlquery
        :param cqlengine.query.DMLQuery dmlquery: the DMLQuery that new event is created on
        """
        self.dmlquery = dmlquery

    @property
    def model(self):
        return self.dmlquery.model

    @property
    def instance(self):
        return self.dmlquery.instance

    @property
    def batch(self):
        return self.dmlquery._batch

    @property
    def ttl(self):
        return self.dmlquery._ttl

    @property
    def consistency(self):
        return self.dmlquery._consistency

    @property
    def timestamp(self):
        return self.dmlquery._timestamp

    @classmethod
    def notify(cls, dmlquery):
        """
        Create new event from dmlquery and notify it
        :param cqlengine.query.DMLQuery dmlquery: the DMLQuery that new event is created on
        """
        cls._subscribers.notify(cls(dmlquery))


def add_subscriber(subscriber, *events):
    """
    Add a subscriber to an event
    :param callable subscriber: subscriber to be added
    :param list[type] events: the classes of events that subscriber want to subscribe on
    """
    for event in events:
        event._subscribers.add(subscriber)


def subscriber(*events):
    """
    Decorator will add the function
    being decorated as an event subscriber for the set events passed
    to the decorator arguments.

    More than one event type can be passed into arguments. The
    decorated subscriber will be called for each event type.

    For example:

    .. code-block:: python

       from cqlengine.events import subscriber
       from app.db import CFModel

       @subscriber(CFModel.AfterSave, CFModel.AfterUpdate)
       def mysubscriber(event):
           print 'Model "%s" has just changed through instance "%s"' \
                % (event.model, event.instance)

    :param list<ModelEvent> events: the events to be add subscriber on
    """
    def _register_subscriber(fn):
        add_subscriber(fn, *events)
        return fn

    assert len(events), 'You have to specify at least an event'
    return _register_subscriber