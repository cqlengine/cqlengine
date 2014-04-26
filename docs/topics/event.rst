=====
Event
=====

.. module:: cqlengine.events

This feature allows cqlengine to notify events when a model's instance got
*save/update/delete*.

    **Note**: It does not work with update/delete by ModelQuery likes without
    model object instance.


Available events
================

+------------------------+----------------------------------------------------------+
| Name                   | Notified on                                              |
+========================+==========================================================+
| ``Model.BeforeSave``   | Before the ``.save()`` or ``.create()`` function is      |
|                        | called.                                                  |
+------------------------+----------------------------------------------------------+
| ``Model.AfterSaved``   | After the ``.save()`` or ``.create()`` function is       |
|                        | called successfully.                                     |
+------------------------+----------------------------------------------------------+
| ``Model.BeforeUpdate`` | Before the ``update()`` function is called.              |
+------------------------+----------------------------------------------------------+
| ``Model.AfterUpdated`` | After the ``update()`` function is called successfully.  |
+------------------------+----------------------------------------------------------+
| ``Model.BeforeDelete`` | Before the ``delete()`` function is called.              |
+------------------------+----------------------------------------------------------+
| ``Model.AfterDeleted`` | After the ``delete()`` function is called successfully.  |
+------------------------+----------------------------------------------------------+

Add subscriber to events
========================

You can add a subscriber to 1+ events by call ``cqlengine.events.add_subscriber``
or use decorator ``cqlengine.events.subscriber`` for the subscriber function.

Examples
--------

.. code-block:: python

    from cqlengine.models import Model, columns
    from cqlengine.events import subscriber, add_subscriber

    class Person(Model):
        first_name  = columns.Text()
        last_name = columns.Text()

    @subscriber(Person.AfterSaved) # add subscriber by decorator
    def on_person_saved(e):
        print e

    def before_person_save(e):
        print e

    @subscriber(Person.AfterSaved, Person.AfterUpdated)
    def after_person_changed(e):
        pass

    add_subscriber(before_person_save, Person.BeforeSave) # add subscriber by function

    p = Person(first_name='John')
    p.save() # ``before_person_save``, ``on_person_saved``, ``after_person_changed`` will be called in order.

    p.update(last_name='Eggleston') # ``after_person_changed`` will be called


APIs
====

.. function:: add_subscriber(subscriber, \*events)

    Add a subscriber to an event
    :param callable subscriber: subscriber to be added
    :param list[type] events: the classes of events that subscriber want to subscribe on

.. decorator:: subscriber(\*events)

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


.. class:: ModelEvent(dmlquery)

    create new event instance from a dmlquery
    :param cqlengine.query.DMLQuery dmlquery: the DMLQuery that new event is created on

    .. attribute:: model

        The class of the object that event is created on its change.

    .. attribute:: instance

        The install of the object that event is created on its change.

    .. attribute:: batch

        The BatchQuery instance that current object is being changed on, ``None``
        if there is no batch.
