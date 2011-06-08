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
