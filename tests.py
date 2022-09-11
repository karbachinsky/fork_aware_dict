from fork_aware_dict import ForkAwareDict
import json


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


def test_with_complex_encoder():
    data = [
        {"word": "foo", "data": {"x": 0, "y": 0, "payload": "x"*1000}},
        {"word": "bar", "data": {"x": 0, "y": 1, "payload": "y"*1000}},
        {"word": "baz", "data": {"x": 1, "y": 0, "payload": "z"*1000}},
    ]

    filename: str = ForkAwareDict.create(
        data,
        key_function=lambda entry: entry["word"],
        encoder=lambda entry: json.dumps(entry["data"]).encode("utf-8")
    )

    index = ForkAwareDict(
        filename=filename,
        decoder=lambda data: json.loads(data)
    )

    assert index.get("bar") == {"x": 0, "y": 1, "payload": "y"*1000}




