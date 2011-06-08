# -*- coding: utf-8 -*-
'''
    minimongo
    ~~~~~~~~~

    Minimongo is a lightweight, schemaless, Pythonic Object-Oriented
    interface to MongoDB.
'''
from __future__ import unicode_literals, absolute_import

__all__ = ('Collection', 'Index', 'Model', 'Rule', 'AttrDict', 'connect')

from .index import Index
from .collection import Collection
from .model import AttrDict, Model, ModelBase
from .meta import Rule


def connect(database, host='localhost', port=27017, lazy=True):
    """Creates a module-wide connection to MongoDB.

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


# Monkey-patching hell! C-BSON parser doesn't support anything but
# `dict`, so we are forced to do some dirty stuff, if we want to
# use other Mapping suptypes.
import bson
import struct
from collections import Mapping

def _dict_to_bson(dict, check_keys, top_level=True):
    try:
        elements = b""
        if top_level and "_id" in dict:
            elements += bson._element_to_bson("_id", dict["_id"], False)
        for (key, value) in dict.iteritems():
            if not top_level or key != "_id":
                elements += bson._element_to_bson(key, value, check_keys)
    except AttributeError:
        raise TypeError("encoder expected a mapping type but got: %r" % dict)

    length = len(elements) + 5
    return struct.pack(b"<i", length) + elements + b"\x00"

bson._dict_to_bson = _dict_to_bson

def _element_to_bson(key, value, check_keys):
    try:
        return _old_element_to_bson(key, value, check_keys)
    except bson.InvalidDocument:
        name = bson._make_c_string(key, True)
        if isinstance(value, Mapping):
            return b"\x03" + name + _dict_to_bson(value, check_keys, False)

_old_element_to_bson = bson._element_to_bson
bson._element_to_bson = _element_to_bson
