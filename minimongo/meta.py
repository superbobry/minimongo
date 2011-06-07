# -*- coding: utf-8 -*-

import types
from collections import namedtuple

from minimongo.collection import Collection


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
