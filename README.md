# Python copy-on-write memory leak free dict 
For really huge dictionaries used in multi-process python programs.

Here the article describing python copy-on-write problem:

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

Run tests:
```buildoutcfg
python -m pytest tests.py
```

**TODO**
- Support not only str keys.
- Think about keys and their leakage. 
