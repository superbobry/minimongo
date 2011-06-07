# -*- coding: utf-8 -*-
from __future__ import with_statement

import operator

import pytest

from bson import DBRef
from minimongo import Collection, Index, Model, Rule, connect
from pymongo.errors import DuplicateKeyError


def setup_function(func):
    connect('minimongo_test')

    for model in Model.__subclasses__() + [TestDerivedModel,
                                           TestModelImplementation]:
        try:
            model.collection.drop()
        except AttributeError:
            continue

    TestModel.auto_index()
    TestModelUnique.auto_index()


class TestCollection(Collection):
    def custom(self):
        return 'It works!'


class TestModel(Model):
    '''Model class for test cases.'''
    class Meta:
        collection = 'minimongo_test'
        indices = (
            Index('x'),
        )

    def a_method(self):
        self.x = 123
        self.y = 456
        self.save()


class TestModelCollection(Model):
    '''Model class with a custom collection class.'''
    class Meta:
        collection = 'minimongo_collection'
        collection_class = TestCollection


class TestModelUnique(Model):
    class Meta:
        collection = 'minimongo_unique'
        indices = (
            Index('x', unique=True),
        )


class TestDerivedModel(TestModel):
    class Meta:
        collection = 'minimongo_derived'


class TestNoAutoIndexModel(Model):
    class Meta:
        collection = 'minimongo_noidex'
        indices = (
            Index('x'),
        )
        auto_index = False


class TestModelInterface(Model):
    class Meta:
        abstract = True


class TestModelImplementation(TestModelInterface):
    class Meta:
        collection = 'minimongo_impl'


class TestFieldMapper(Model):
    class Meta:
        database = 'minimongo_test'
        collection = 'minimongo_mapper'
        field_map = [
            Rule(lambda k, v: k == 'x' and isinstance(v, int),
                 lambda v: float(v * (4.0 / 3.0)))
        ]


def test_meta():
    assert hasattr(TestModel, '_meta')
    assert not hasattr(TestModel, 'Meta')

    meta = TestModel._meta

    for attr in ('indices', 'collection', 'collection_class'):
        assert hasattr(meta, attr)

    assert meta.collection == 'minimongo_test'
    assert meta.indices == (Index('x'), )


def test_dictyness():
    item = TestModel({'x': 642})

    assert item['x'] == item.x == 642

    item.y = 426
    assert item['y'] == item.y == 426

    assert set(item.keys()) == set(['x', 'y'])

    del item['x']
    assert item == {'y': 426}
    item.z = 3
    del item.y
    assert item == {'z': 3}


def test_creation():
    '''Test simple object creation and querying via find_one.'''
    object_a = TestModel({'x': 1, 'y': 1})
    object_a.z = 1
    object_a.save()

    object_b = TestModel.collection.find_one({'x': 1})

    # Make sure that the find_one method returns the right type.
    assert isinstance(object_b, TestModel)
    # Make sure that the contents are the same.
    assert object_b == object_a

    # Make sure that our internal representation is what we expect (and
    # no extra fields, etc.)
    assert object_a == {'x': 1, 'y': 1, 'z': 1, '_id': object_a._id}
    assert object_b == {'x': 1, 'y': 1, 'z': 1, '_id': object_b._id}


def test_find_one():
    model = TestModel({'x': 1, 'y': 1})
    model.save()

    assert model._id is not None

    found = TestModel.collection.find_one(model._id)
    assert found is not None
    assert isinstance(found, TestModel)
    assert found == model


def test_save_with_arguments():
    # Manipulate is what inserts the _id on save if it is missing
    model = TestModel(foo=0)
    model.save(manipulate=False)

    with pytest.raises(AttributeError):
        model._id

    # but the object was actually saved
    model = TestModel.collection.find_one({'foo': 0})
    assert model.foo == 0


def test_index_existance():
    '''Test that indexes were created properly.'''
    indices = TestModel.collection.index_information()
    assert indices['x_1'] == {'key': [('x', 1)], 'v': 0}


def test_unique_index():
    '''Test behavior of indices with unique=True'''
    # This will work (y is undefined)
    TestModelUnique({'x': 1}).save()
    TestModelUnique({'x': 1}).save()
    # Assert that there's only one object in the collection, even though
    # we inserted two.  The uniqueness constraint on the index has dropped
    # one of the inserts (silently, I guess).
    assert TestModelUnique.collection.find().count() == 1

    # Even if we use different values for y, it's still only one object:
    TestModelUnique({'x': 2, 'y': 1}).save()
    TestModelUnique({'x': 2, 'y': 2}).save()
    # There are now 2 objects, one with x=1, one with x=2.
    assert TestModelUnique.collection.find().count() == 2


def test_unique_constraint():
    x1_a = TestModelUnique({'x': 1, 'y': 1})
    x1_b = TestModelUnique({'x': 1, 'y': 2})
    x1_a.save(safe=True)

    with pytest.raises(DuplicateKeyError):
        x1_b.save(safe=True)

    x1_c = TestModelUnique({'x': 2, 'y': 1})
    x1_c.save()


def test_queries():
    '''Test some more complex query forms.'''
    a, b, c, d = [
        TestModel({'x': 1, 'y': 1}).save(),
        TestModel({'x': 1, 'y': 2}).save(),
        TestModel({'x': 2, 'y': 2}).save(),
        TestModel({'x': 2, 'y': 1}).save()
    ]

    x1 = list(TestModel.collection.find({'x': 1}))
    y1 = list(TestModel.collection.find({'y': 1}))
    x2y2 = list(TestModel.collection.find({'x': 2, 'y': 2}))

    # make sure the types of the things coming back from find() are the
    # derived Model types, not just a straight dict.
    assert all(map(lambda x: isinstance(x, TestModel), x1 + y1 + x2y2))
    assert x1 == [a, b]
    assert y1 == [a, d]
    assert x2y2 == [c]


def test_deletion():
    '''Test deleting an object from a collection.'''
    object_a = TestModel()
    object_a.x = 100
    object_a.y = 200
    object_a.save()

    object_b = TestModel.collection.find({'x': 100})
    assert object_b.count() == 1

    map(operator.methodcaller('remove'), object_b)

    object_a = TestModel.collection.find({'x': 100})
    assert object_a.count() == 0


def test_complex_types():
    '''Test lists as types.'''
    object_a = TestModel()
    object_a.l = ['a', 'b', 'c']
    object_a.x = 1
    object_a.y = {'m': 'n',
                  'o': 'p'}
    object_a['z'] = {'q': 'r',
                     's': {'t': 'u'}}

    object_a.save()

    object_b = TestModel.collection.find_one({'x': 1})

    # Make sure the internal lists are equivalent.
    assert object_a.l == object_b.l

    # Make sure that everything is of the right type, including the types of
    # the nested fields that we read back from the DB, and that we are able
    # to access fields as both attrs and items.
    assert type(object_a) == type(object_b) == TestModel
    assert isinstance(object_a.y, dict)
    assert isinstance(object_b.y, dict)
    assert isinstance(object_a['z'], dict)
    assert isinstance(object_b['z'], dict)
    assert isinstance(object_a.z, dict)
    assert isinstance(object_b.z, dict)

    # These nested fields are actually instances of AttrDict, which is why
    # we can access as both attributes and values.  Thus, the "isinstance"
    # dict check.
    assert isinstance(object_a['z']['s'], dict)
    assert isinstance(object_b['z']['s'], dict)
    assert isinstance(object_a.z.s, dict)
    assert isinstance(object_b.z.s, dict)

    assert object_a == object_b


def test_type_from_cursor():
    TestModel({'x':1}).save()
    TestModel({'x':2}).save()
    TestModel({'x':3}).save()
    TestModel({'x':4}).save()
    TestModel({'x':5}).save()

    objects = TestModel.collection.find()
    for single_object in objects:
        assert type(single_object) == TestModel
        # Make sure it's both a dict and a TestModel, which is also an object
        assert isinstance(single_object, dict)
        assert isinstance(single_object, object)
        assert isinstance(single_object, TestModel)
        assert type(single_object['x']) == int


def test_delete_field():
    '''Test deleting a single field from an object.'''
    object_a = TestModel({'x': 1, 'y': 2})
    object_a.save()
    del object_a.x
    object_a.save()

    assert TestModel.collection.find_one({'y': 2}) == \
           {'y': 2, '_id': object_a._id}


def test_count_and_fetch():
    '''Test counting methods on Cursors. '''
    object_d = TestModel({'x': 1, 'y': 4}).save()
    object_b = TestModel({'x': 1, 'y': 2}).save()
    object_a = TestModel({'x': 1, 'y': 1}).save()
    object_c = TestModel({'x': 1, 'y': 3}).save()

    find_x1 = TestModel.collection.find({'x': 1}).sort('y')
    assert find_x1.count() == 4

    list_x1 = list(find_x1)
    assert list_x1[0] == object_a
    assert list_x1[1] == object_b
    assert list_x1[2] == object_c
    assert list_x1[3] == object_d


def test_fetch_and_limit():
    '''Test counting methods on Cursors. '''
    object_a = TestModel({'x': 1, 'y': 1}).save()
    object_b = TestModel({'x': 1, 'y': 2}).save()
    TestModel({'x': 1, 'y': 4}).save()
    TestModel({'x': 1, 'y': 3}).save()

    find_x1 = TestModel.collection.find({'x': 1}).limit(2).sort('y')

    assert find_x1.count(with_limit_and_skip=True) == 2
    assert object_a in find_x1
    assert object_b in find_x1


def test_dbref():
    '''Test generation of DBRef objects, and querying via DBRef
    objects.'''
    object_a = TestModel({'x': 1, 'y': 999}).save()
    ref_a = object_a.dbref()

    object_b = TestModel.collection.from_dbref(ref_a)
    assert object_a == object_b

    # Making sure, that a ValueError is raised for DBRefs from a
    # 'foreign' collection or database.
    with pytest.raises(ValueError):
        ref_a = DBRef('foo', ref_a.id)
        TestModel.collection.from_dbref(ref_a)

    with pytest.raises(ValueError):
        ref_a = DBRef(ref_a.collection, ref_a.id, 'foo')
        TestModel.collection.from_dbref(ref_a)

    # Testing ``with_database`` option.
    ref_a = object_a.dbref(with_database=False)
    assert ref_a.database is None

    ref_a = object_a.dbref(with_database=True)
    assert ref_a.database is not None

    ref_a = object_a.dbref()  # True by default.
    assert ref_a.database is not None

    # Testing additional fields
    ref_a = object_a.dbref(name="foo")
    assert ref_a.name == 'foo'


def test_db_and_collection_names():
    assert TestModel.collection.name == 'minimongo_test'
    assert TestModelCollection.collection.name == 'minimongo_collection'


def test_derived():
    [m.remove() for m in TestDerivedModel.collection.find()]

    derived_object = TestDerivedModel()
    derived_object.a_method()

    assert derived_object.__class__.collection.name == 'minimongo_derived'
    assert TestDerivedModel.collection.find_one({'x': 123}) == derived_object


def test_collection_class():
    model = TestModelCollection

    assert isinstance(model.collection, TestCollection)
    assert hasattr(model.collection, 'custom')
    assert model.collection.custom() == 'It works!'


def test_str_and_unicode():
    assert str(TestModel()) == 'TestModel({})'
    assert str(TestModel({'foo': 'bar'})) == 'TestModel({\'foo\': \'bar\'})'

    assert unicode(TestModel({'foo': 'bar'})) == \
           u'TestModel({\'foo\': \'bar\'})'

    # __unicode__() doesn't decode any bytestring values to unicode,
    # leaving it up to the user.
    assert unicode(TestModel({'foo': '←'})) ==  \
           u'TestModel({\'foo\': \'\\xe2\\x86\\x90\'})'
    assert unicode(TestModel({'foo': u'←'})) == \
           u'TestModel({\'foo\': u\'\\u2190\'})'


def test_auto_collection_name():
    try:
        class SomeModel(Model):
            pass
    except Exception:
        pytest.fail('`collection_name` should\'ve been constructed.')

    assert SomeModel._meta.collection == 'some_model'


def test_no_auto_index():
    TestNoAutoIndexModel({'x': 1}).save()

    assert TestNoAutoIndexModel.collection.index_information() == \
           {u'_id_': {u'key': [(u'_id', 1)], 'v': 0}}

    TestNoAutoIndexModel.auto_index()

    assert TestNoAutoIndexModel.collection.index_information() == \
           {u'_id_': {u'key': [(u'_id', 1)], 'v': 0},
            u'x_1': {u'key': [(u'x', 1)], 'v': 0}}


def test_interface_models():
    test_interface_instance = TestModelInterface()
    test_interface_instance.x = 5

    with pytest.raises(AttributeError):
        test_interface_instance.save()

    test_model_instance = TestModelImplementation()
    test_model_instance.x = 123
    test_model_instance.save()

    test_model_instance_2 = TestModelImplementation.collection.find_one(
        {'x': 123})
    assert test_model_instance == test_model_instance_2


def test_slicing():
    xs = []
    for x in xrange(5):
        xs.append(TestModel(x=x).save())

    objects = TestModel.collection.find().sort('x')
    obj_list = list(objects[:2])
    assert obj_list == xs[:2]
    assert all(map(lambda x: isinstance(x, TestModel), obj_list))

    # We can't re-slice an already sliced cursor, so we query again.
    objects = TestModel.collection.find().sort('x')
    obj_list = list(objects[2:])
    assert obj_list == xs[2:]
    assert all(map(lambda x: isinstance(x, TestModel), obj_list))

def test_field_mapper():
    test_mapped_object = TestFieldMapper()
    # x is going to be multiplied by 4/3 automatically.
    test_mapped_object.x = 6
    test_mapped_object.y = 7
    test_mapped_object.z = 6.0
    assert test_mapped_object.x == 8.0
    assert test_mapped_object.y == 7
    assert test_mapped_object.z == 6.0
    assert type(test_mapped_object.x) == float
    assert type(test_mapped_object.y) == int
    assert type(test_mapped_object.z) == float
    test_mapped_object.save()

    loaded_mapped_object = TestFieldMapper.collection.find_one()

    # When the object was loaded from the database, the mapper automatically
    # multiplied every integer field by 4.0/3.0 and converted it to a float.
    # This is a crazy use case only used for testing here.
    assert test_mapped_object.x == 8.0
    assert test_mapped_object.y == 7
    assert test_mapped_object.z == 6.0

    assert type(loaded_mapped_object.x) == float
    assert type(test_mapped_object.x) == float

    assert type(loaded_mapped_object.y) == int
    assert type(loaded_mapped_object.z) == float
