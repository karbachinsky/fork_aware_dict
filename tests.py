from fork_aware_dict import ForkAwareDict


def test_with_simple_dict():
    data = {
        "foo": "aaa",
        "bar": "bbbb",
        "baz": "ccccc"
    }

    filename: str = ForkAwareDict.create(data.items())

    index = ForkAwareDict(filename=filename)

    assert index.get("foo") == "aaa"
    assert index.get("bar") == "bbbb"
    assert index.get("baz") == "ccccc"
    assert index.get("something") is None


