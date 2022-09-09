from mtmlf_dict import MtmlfDict


def test_with_simple_dict():
    data = {
        "foo": "aaa",
        "bar": "bbbb",
        "baz": "ccccc"
    }

    filename: str = MtmlfDict.create(data)

    index = MtmlfDict(filename)

    assert index.get("foo") == "aaa"
    assert index.get("bar") == "bbbb"
    assert index.get("baz") == "ccccc"
    assert index.get("something") is None
