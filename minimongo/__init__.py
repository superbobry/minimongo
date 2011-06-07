# -*- coding: utf-8 -*-
'''
    minimongo
    ~~~~~~~~~

    Minimongo is a lightweight, schemaless, Pythonic Object-Oriented
    interface to MongoDB.
'''
from __future__ import unicode_literals, absolute_import

__all__ = ('Collection', 'Index', 'Model', 'connect')

from .index import Index
from .collection import Collection
from .model import Model, ModelBase


def connect(database, host='localhost', port=27017):
    """Creates a module-wide connection to MongoDB.

    :param unicode database: database name to connect to.
    :param unicode host: MongoDB server hostname.
    :param int port: MongoDB server port.
    """
    from pymongo import Connection
    ModelBase.db = Connection(host, port, _connect=False)[database]

    # Iterate over all of the already declared models and auto index
    # them.
    for model in Model.__subclasses__():
        try:
            if model._meta.auto_index:
                model.auto_index()
        except AttributeError:
            pass  # Nasty abstract models don't have no ``_meta``.

    return ModelBase.db

