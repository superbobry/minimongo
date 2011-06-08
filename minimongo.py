# -*- coding: utf-8 -*-

from __future__ import unicode_literals

__all__ = [
    "connect", "configure",
    "Collection", "Model", "AttrDict", "Index", "Rule"
]

import re
import types
from abc import ABCMeta
from collections import namedtuple, MutableMapping

from bson import DBRef, ObjectId
from pymongo.collection import Collection as _Collection
from pymongo.cursor import Cursor as _Cursor


class NotConnected(Exception):
    """Exception raised, when :attr:`Model.collection` is accessed,
    before :func:`.connect` has been called.
    """


class Index(object):
    """A simple wrapper for arguments to
    :meth:`pymongo.collection.Collection.ensure_index`."""

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def __eq__(self, other):
        """Two indices are equal, when the have equal arguments.

        >>> Index(42, foo='bar') == Index(42, foo='bar')
        True
        >>> Index(foo='bar') == Index(42, foo='bar')
        False

        :param Index other: an index to compare with.
        """
        return self.__dict__ == other.__dict__

    def ensure(self, collection):
        """Calls :meth:`pymongo.collection.Collection.ensure_index`
        on the given `collection` with the stored arguments.
        """
        return collection.ensure_index(*self._args, **self._kwargs)


class Cursor(_Cursor):
    def __init__(self, *args, **kwargs):
        self._wrapper_class = kwargs.pop('wrap')
        super(Cursor, self).__init__(*args, **kwargs)

    def next(self):
        data = super(Cursor, self).next()
        return self._wrapper_class(data)

    def __getitem__(self, index):
        item = super(Cursor, self).__getitem__(index)

        if isinstance(index, slice):
            return item
        else:
            return self._wrapper_class(item)


class CollectionDescriptor(object):
    def __get__(self, instance, model=None):
        if model.db is None:
            raise NotConnected
        elif instance is not None:
            raise AttributeError("Collection isn't accessible from {0} "
                                 "instances.".format(model))
        elif not hasattr(model, "_meta"):
            raise AttributeError("Manager isn't accessible from abstract "
                                 "models.")
        else:
            return model._meta.collection_class(
                model.db, model._meta.collection, document_class=model)


class Collection(_Collection):
    """A wrapper around :class:`pymongo.collection.Collection` that
    provides the same functionality, but stores the document class of
    the collection we're working with. So that
    :meth:`pymongo.collection.Collection.find` and
    :meth:`pymongo.collection.Collection.find_one` can return the right
    classes instead of plain :class:`dict`.
    """

    #: A reference to the model class, which uses this collection.
    document_class = None

    def __init__(self, *args, **kwargs):
        self.document_class = kwargs.pop('document_class')
        super(Collection, self).__init__(*args, **kwargs)

    def find(self, *args, **kwargs):
        """Same as :meth:`pymongo.collection.Collection.find`, except
        it returns the right document class.
        """
        return Cursor(self, *args, wrap=self.document_class, **kwargs)

    def find_one(self, *args, **kwargs):
        """Same as :meth:`pymongo.collection.Collection.find_one`, except
        it returns the right document class.
        """
        data = super(Collection, self).find_one(*args, **kwargs)
        if data:
            return self.document_class(data)
        return None

    def from_dbref(self, dbref):
        """Given a :class:`pymongo.dbref.DBRef`, dereferences it and
        returns a corresponding document, wrapped in an appropriate model
        class.

        .. note:: If a given `dbref` point to a different database and
                  / or collection, :exc:`ValueError` is raised.
        """
        # Making sure a given DBRef points to a proper collection
        # and database.
        if not dbref.collection == self.name:
            raise ValueError('DBRef points to an invalid collection.')
        elif dbref.database and not dbref.database == self.database.name:
            raise ValueError('DBRef points to an invalid database.')
        else:
            return self.find_one(dbref.id)


#: Field convertion rule. `match` is a function, which checks if a
#: `converter` function should be applied to a given field. `converter`
#: *as already noted* takes current field value and returns a new
#: one.
Rule = namedtuple("Rule", "match converter")


class Meta(object):
    """Container class for :class:`Model` metadata."""

    #: Indexes that should be generated for this model
    indices = ()

    #: Current database and connection
    collection = None

    #: Should indices be created at startup?
    auto_index = True

    #: Should we connect implicitly on first :attr:`Model.collection`
    #: access?
    auto_connect = True

    #: A list of :class:`Rule` instances, used for type coercions.
    field_map = []

    #: What is the base class for Collections.
    collection_class = Collection

    #: Should we treat this model as abstract? i.e. not linked to any
    #: particular collection.
    abstract = False

    def __init__(self, meta):
        if meta is not None:
            self.__dict__.update(meta.__dict__)

    @classmethod
    def configure(cls, **defaults):
        """Updates class-level defaults for :class:`Meta` container."""
        for attr, value in defaults.iteritems():
            setattr(cls, attr, value)


class ModelBase(ABCMeta):
    #: A reference to MongoDB connection (is set by :func:`.connect`).
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
        else:
            new_class._meta = None

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


class AttrDict(MutableMapping):
    """A dict with attribute access.

    >>> d = AttrDict({"foo": "bar"}, baz=-1)
    >>> d.foo
    'bar'
    >>> d.baz = 0
    >>> d
    {'foo': 'bar', 'baz': 0}

    Note, that even though ``AttrDict`` looks and behaves exactly like
    a :func:`dict` -- it's in fact a concrete
    :class:`collections.MutableMapping` implementation.

    >>> isinstance(AttrDict(), dict)
    False
    >>> issubclass(AttrDict, dict)
    False

    However, if you need a **read-only** "dicty" version of an
    ``AttrDict``, use :attr:`container` attribute:

    >>> isintance(AttrDict().container, dict)
    True
    """

    def __init__(self, initial=None, **kwargs):
        self.__dict__["_container"] = {}

        if initial:
            self.update(initial)
        if kwargs:
            self.update(kwargs)

    for method in ["__iter__", "__repr__", "__str__", "__len__",
                   "__getitem__", "__delitem__"]:
        exec("{method} = lambda self, *args: self._container.{method}(*args)"
             .format(**locals()))

    def __setitem__(self, key, value):
        if isinstance(value, dict):
            value = AttrDict(value)

        self._container[key] = value

    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            raise AttributeError(attr)

    def __setattr__(self, attr, value):
        self[attr] = value

    def __delattr__(self, attr):
        try:
            del self[attr]
        except KeyError:
            raise AttributeError(attr)

    def _get_container(self):
        data = {}
        for key, value in self.iteritems():
            if isinstance(value, AttrDict):
                value = value.container

            data[key] = value

        return data
    container = property(_get_container,
        doc="""A **read-only** version of ``AttrDict`` contents.""")

MutableMapping.register(AttrDict)


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

    #: A proxy to the related MongoDB collection, which is either
    #: constructed from class name ``class Foo: -- "foo"`` or set
    #: explicitly in the :class:`.meta.Meta` class.
    collection = CollectionDescriptor()

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__,
                           super(Model, self).__str__())

    def __setitem__(self, key, value):
        if self._meta and self._meta.field_map:
            for rule in self._meta.field_map:
                if rule.match(key, value):
                   value = rule.converter(value)
                   break

        super(Model, self).__setitem__(key, value)

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

    def save(self, **kwargs):
        """Saves this model from the related collection."""
        _id = self.__class__.collection.save(self.container, **kwargs)

        # Thanks to the nice ternary operator fail in Pymongo -- we have
        # to check that ourselves.
        if isinstance(_id, list): _id = _id[0]

        if _id is not None:
            self.update(_id=_id)
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


def configure(module=None, prefix='MONGODB_', **kwargs):
    """Sets defaults for ``class Meta`` declarations.

    Arguments can either be extracted from a `module` (in that case
    all attributes starting from `prefix` are used):

    >>> import foo
    >>> configure(foo)

    or passed explicictly as keyword arguments:

    >>> configure(database='foo')

    .. warning:: Current implementation is by no means thread-safe --
                 use it wisely.
    """
    if module is not None and isinstance(module, types.ModuleType):
        # Search module for MONGODB_* attributes and converting them
        # to Meta' values, ex: MONGODB_PORT ==> port.
        attrs = module.__dict__.iteritems()
        attrs = ((attr.replace(prefix, '').lower(), value)
                 for attr, value in attrs if attr.startwith(prefix))

        Meta.configure(**dict(attrs))
    elif kwargs:
        Meta.configure(**kwargs)


def connect(database, host='localhost', port=27017, lazy=True):
    """Creates a module-wide connection to MongoDB.

    >>> c =connect("foo", lazy=False)
    Database(Connection(u'localhost', 27017), u'foo')
    >>> c = connect("foo")
    Database(Connection(None, None), u'foo')

    Note, that in the latter example we got a "lazy" connection, which
    has not yet been initialized -- but once we access any of the
    :class:`.collection.Collection` methods the connection is
    established:

    >>> c.collection_names()
    [u'bar', u'system.indexes']
    >>> c
    Database(Connection(u'localhost', 27017), u'foo')

    :param unicode database: database name to connect to.
    :param unicode host: MongoDB server hostname.
    :param int port: MongoDB server port.
    :param bool lazy: don't connect to the database, unless it's
                      required.
    """
    from pymongo import Connection
    ModelBase.db = Connection(host, port, _connect=not lazy)[database]

    # Iterate over all of the already declared models and auto index
    # them.
    for model in Model.__subclasses__():
        try:
            if model._meta.auto_index:
                model.auto_index()
        except AttributeError:
            pass  # Nasty abstract models don't have no ``_meta``.

    return ModelBase.db
