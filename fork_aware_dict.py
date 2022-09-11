import mmap
import struct
import tempfile
import zlib
from typing import (
    Any,
    Callable,
    Generator,
    Hashable,
    Iterable,
    Optional,
    Union,
    List,
    Dict,
    Tuple,
)


class ForkAwareDict:
    """
    Consider you have a huge dictionary.
    Keys are strings and values are some complex structures.
    You can fast search in this dict, but there is a problem with it:
    Once you fork the process you may expect that this dict will not to be copied
    into the child process because there is a Linux CoW mechanism.
    Actually it is true but at Python it doesn't work as you expect.
    Here is ant article explaining the problem:

    Usage example:
    filename: str = ForkAwareDict.create(
        {
            "foo": "aaa",
            "bar": "bbbb",
            "baz": "ccccc",
        }.items()
    )

    index = ForkAwareDict(filename=filename)

    assert index.get("bar") == "bbbb"

    Complex cases:
    Once values at your dictionary are not simple string, use can use custom encoding:

    Notes:
    – Our implementation also compresses the data for
          each key which gave as 80% less memory.
    – This implementation is able to use mmap to get rid of slow dict operations,
          which somehow decreases the read speed.
    """

    BUF_SIZE = 4096

    class Error(Exception):
        pass

    def __init__(
        self,
        *,
        filename: str,
        in_memory: bool = True,
        decoder: Callable[[bytes], Any] = lambda e: e.decode("utf-8"),
    ) -> None:
        """
        :param filename:
            Filename containing binary index
        :param in_memory:
            If False then mmap will be used. It may slow down the search.
            Use it for really huge index
        :param decoder:
            Function that determines how to convert row binary into proper python structure
        """
        self.decoder = decoder

        self.plain_index: Dict[str, Tuple[int, int]] = {}

        with open(filename, "r+b") as f:
            try:
                # memory-map the file, size 0 means the whole file
                if not in_memory:
                    self._idx = mmap.mmap(f.fileno(), 0)
                else:
                    self._idx = f.read()

                num_records = struct.unpack("Q", self._idx[:8])[0]
                keys_offsets: List[int] = []
                for i in range(num_records + 1):
                    offset = struct.unpack("Q", self._idx[(i + 1) * 8 : (i + 2) * 8])[0]

                    keys_offsets.append(offset)

                keys_start = 8 * (num_records + 2)
                data_keys_offsets_start = keys_start + keys_offsets[-1]
                data_start_offset = data_keys_offsets_start + (num_records + 1) * 8

                for i in range(num_records):
                    key = self._idx[
                        keys_start + keys_offsets[i] : keys_start + keys_offsets[i + 1]
                    ].decode("utf-8")

                    start = struct.unpack(
                        "Q",
                        self._idx[
                            data_keys_offsets_start
                            + i * 8 : data_keys_offsets_start
                            + (i + 1) * 8
                        ],
                    )[0]

                    end = struct.unpack(
                        "Q",
                        self._idx[
                            data_keys_offsets_start
                            + (i + 1) * 8 : data_keys_offsets_start
                            + (i + 2) * 8
                        ],
                    )[0]

                    self.plain_index[key] = (
                        data_start_offset + start,
                        data_start_offset + end,
                    )

            except ValueError as e:
                raise self.Error(f"Broken file format: {e}")

    def get(self, key: Hashable, default: Any = None) -> Optional[Any]:
        """
        :param key: an id at plain index to get an offset
            Key to search
        :param default
            Default value to return if key is not found in index
        """
        offsets = self.plain_index.get(key)
        if offsets is None:
            return default

        binary_data = self._idx[offsets[0] : offsets[1]]

        try:
            binary_data = self.decoder(zlib.decompress(binary_data))
            return binary_data
        except ValueError:
            return default

    @classmethod
    def create(
        cls,
        iterable: Union[Iterable, Generator],
        *,
        key_function: Callable[[Any], str] = lambda x: x[0],
        encoder: Callable[[Any], bytes] = lambda e: e[1].encode("utf-8"),
    ) -> Optional[str]:
        """
        Given an iterable of anything
        creates BinaryIndex for it.

        :param iterable:
            Iterable Data for indexation. Like dict.items()

        :param key:
            Function that determines how to extract the key from each row of data

        :param encoder:
            Function that determines how to convert row data into bytes

        return: temporary filename with binary index
        """
        keys_fp = tempfile.NamedTemporaryFile(
            suffix=".keys.bin", delete=False, mode="w+b"
        )
        keys_offset_fp = tempfile.NamedTemporaryFile(
            suffix=".keys.offsets.bin", delete=False, mode="w+b"
        )
        data_fp = tempfile.NamedTemporaryFile(
            suffix=".data.bin", delete=False, mode="w+b"
        )
        data_offset_fp = tempfile.NamedTemporaryFile(
            suffix=".data.offsets.bin", delete=False, mode="w+b"
        )

        num_records = 0
        offset = 0
        keys_offset = 0
        for entry in iterable:
            num_records += 1
            data_offset_fp.write(struct.pack("Q", offset))
            keys_offset_fp.write(struct.pack("Q", keys_offset))

            try:
                key: str = key_function(entry)
                entry: bytes = encoder(entry)
            except Exception as e:
                raise cls.Error(
                    f"Couldn't convert {entry} to bytes: {e}",
                )

            if not isinstance(key, str):
                raise cls.Error(f"Key for entry {entry} must be str instance")

            binary_data = zlib.compress(entry)
            binary_key = key.encode("utf-8")

            data_fp.write(binary_data)
            keys_fp.write(binary_key)

            offset += len(binary_data)
            keys_offset += len(binary_key)

        data_offset_fp.write(struct.pack("Q", offset))
        keys_offset_fp.write(struct.pack("Q", keys_offset))

        data_offset_fp.close()
        data_fp.close()
        keys_offset_fp.close()
        keys_fp.close()

        final_fp = tempfile.NamedTemporaryFile(
            suffix=".full.data", delete=False, mode="w+b"
        )
        final_fp.write(struct.pack("Q", num_records))

        for fp in (
            keys_offset_fp,
            keys_fp,
            data_offset_fp,
            data_fp,
        ):
            with open(fp.name, "r+b") as f:
                buf = f.read(cls.BUF_SIZE)
                while buf:
                    final_fp.write(buf)
                    buf = f.read(cls.BUF_SIZE)

        final_fp.close()

        return final_fp.name
