# -*- coding: utf-8 -*-

import re

from bson import DBRef, ObjectId

from minimongo.meta import Meta


class NotConnected(Exception):
    """Exception raised, when :attr:`Model.collection` is accessed,
    before :func:`.connect` has been called.
    """

class ModelBase(type):
    #: A reference to MongoDB connection (is set by :func:`connect`).
    db = None

    def __new__(mcs, name, bases, attrs):
        new_class = super(ModelBase,
                          mcs).__new__(mcs, name, bases, attrs)
        parents = [b for b in bases if isinstance(b, ModelBase)]
        if not parents:
            # If this isn't a subclass of Model, don't do anything special.
            return new_class

        # Processing Model metadata.
        try:
            meta = getattr(new_class, 'Meta')
        except AttributeError:
            meta = None
        else:
            delattr(new_class, 'Meta')  # Won't need the original metadata
                                        # container anymore.
        finally:
            meta = Meta(meta)

        if not meta.abstract:
            new_class._meta = meta

            meta.collection = meta.collection or to_underscore(name)
            if meta.auto_index and mcs.db:
                new_class.auto_index()

        return new_class

    def auto_index(cls):
        """Builds all indices, listed in model's Meta class.

           >>> class SomeModel(Model)
           ...     class Meta:
           ...         indices = (
           ...             Index('foo'),
           ...         )

        .. note:: this will result in calls to
                  :meth:`pymongo.collection.Collection.ensure_index`
                  method at import time, so import all your models up
                  front.
        """
        for index in cls._meta.indices:
            index.ensure(cls.collection)


class Manager(object):
    def __get__(self, instance, model=None):
        if model.db is None:
            raise NotConnected
        elif instance is not None:
            raise AttributeError("Manager isn't accessible from {0} instances."
                                 .format(model))
        elif not hasattr(model, "_meta"):
            raise AttributeError("Manager isn't accessible from abstract models.")
        else:
            return model._meta.collection_class(
                model.db, model._meta.collection, document_class=model)


class AttrDict(dict):
    """A dict with attribute access.

    >>> d = AttrDict({"foo": "bar"}, baz=-1)
    >>> d.foo
    'bar'
    >>> d.baz = 0
    >>> d
    {'foo': 'bar', 'baz': 0}

    .. todo:: overriding :meth:`dict.__setitem__` is a really-**really**
              bad idea, because all of the dict's methods will simply
              ignore our `__setitem__` -- this needs to be fixed.
    """
    def __init__(self, initial=None, **kwargs):
        # Make sure that during initialization, that we recursively apply
        # AttrDict.
        if initial:
            for key, value in initial.iteritems():
                # Can't just say self[k] = v here b/c of recursion.
                self.__setitem__(key, value)

        # Process the other arguments (assume they are also default values).
        # This is the same behavior as the regular dict constructor.
        for key, value in kwargs.iteritems():
            self.__setitem__(key, value)

        super(AttrDict, self).__init__()

    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            raise AttributeError(attr)

    def __setattr__(self, attr, value):
        try:
            self[attr] = value
        except KeyError:
            raise AttributeError(attr)

    def __delattr__(self, attr):
        try:
            super(AttrDict, self).__delitem__(attr)
        except KeyError:
            raise AttributeError(attr)

    def __setitem__(self, key, value):
        # Coerce all nested dict-valued fields into AttrDicts
        if isinstance(value, dict):
            value = AttrDict(value)

        super(AttrDict, self).__setitem__(key, value)


class Model(AttrDict):
    """Base class for all Minimongo objects.

    >>> class Foo(Model):
    ...     class Meta:
    ...         indices = (
    ...             Index('bar', unique=True),
    ...         )
    ...
    >>> foo = Foo(bar=42)
    >>> foo
    {'bar': 42}
    >>> foo.bar == 42
    True
    """

    __metaclass__ = ModelBase

    #: A Manager which handles all MongoDB interactions.
    collection = Manager()

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__,
                           super(Model, self).__str__())

    def __unicode__(self):
        return str(self).decode('utf-8')

    def dbref(self, with_database=True, **kwargs):
        """Returns a `DBRef` for this model.

        :param bool with_database: if `False`, the resulting
                                   :class:`pymongo.dbref.DBRef` won't
                                   have a :attr:`database` field.

        .. note:: Any additional keyword arguments will be passed to
                  :class:`pymongo.dbref.DBRef` constructor, as per
                  MongoDB specs.
        """
        if not hasattr(self, '_id'):
            self._id = ObjectId()

        if with_database:
            database = self.__class__.collection.database.name
        else:
            database = None

        return DBRef(self._meta.collection, self._id, database, **kwargs)

    def remove(self, *args, **kwargs):
        """Removes this model from the related collection."""
        self.__class__.collection.remove(self._id, *args, **kwargs)
        return self

    def save(self, *args, **kwargs):
        """Saves this model from the related collection."""
        self.__class__.collection.save(self, *args, **kwargs)
        return self


# Utils.

def to_underscore(string):
    """Converts a given string from CamelCase to under_score.

    >>> to_underscore('FooBar')
    'foo_bar'
    """
    new_string = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', string)
    new_string = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', new_string)
    return new_string.lower()
