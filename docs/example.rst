Example
-------

Here's a very brief example of creating an object, querying for it, modifying
a field, and then saving it back again::

    class Foo(Model):
        class Meta:
            # If not set explicitly, collection name will be constructed
            # from class name: "foo" in this case.
            collection = "rocks"

            # Now, we programatically declare what indices we want.
            # The arguments to the Index constructor are identical to
            # the args to pymongo"s ensure_index function.
            indices = [
                Index("a"),
            ]


    if __name__ == "__main__":
        # Connect to the database first.
        connect("test")

        # Create & save an object, and return a local in-memory copy of it:
        foo = Foo({"x": 1, "y": 2}).save()

        # Find that object again, loading it into memory:
        foo = Foo.collection.find_one({"x": 1})

        # Change a field value, and save it back to the DB.
        foo.other = "some data"
        foo.save()
