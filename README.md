# Python copy-on-write memory leak free dict 
For really huge dictionaries used in multi-process python programs.

Here the article describing python copy-on-write memory problem with:
https://karbachinsky.medium.com/python-server-is-gradually-running-out-of-memory-1de8d2b7ef29


Usage:
```python
from fork_aware_dict import  import ForkAwareDict

# Creating index
filename: str = ForkAwareDict.create(
    {
        "foo": "aaa",
        "bar": "bbbb",
        "baz": "ccccc",
    }.items()
)

# Reading and using
index = ForkAwareDict(filename=filename)

assert index.get("bar") == "bbbb"
```

Or using complex iterable data:
```python
    import json

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
```

Run tests:
```buildoutcfg
python -m pytest tests.py
```

**TODO**
- Support not only str keys.
- Think about keys and their leakage. 
