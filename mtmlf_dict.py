import mmap
import struct
import tempfile
import zlib
from typing import Any, Callable, Generator, Hashable, Iterable, Optional, Union, List


class MtmlfDict:
    """
    Consider you have a huge dictionary.
    Keys are strings and values are some complex structures.
    You can fast search in this dict, but there is a problem with it:
    Once you fork the process you may expect that this dict will not to be copied
    into the child process because there is a Linux CoW mechanism.
    Actually it is true but at Python it doesn't work as you expect.
    Here is ant article explaining the problem:

    Usage example:
    filename: str = MtmlfDict.create(
        {
            "foo": "aaa",
            "bar": "bbbb",
            "baz": "ccccc",
        }.items()
    )

    index = MtmlfDict(filename=filename)

    assert index.get("bar") == "bbbb"

    Complex cases:
    Once values at your dictionary are not simple string, use can use custom encoding:

    Notes:
    – Our implementation also compresses the data for each key which gave as 80% less memo.
    – This implementation is able to use mmap to get rid of slow dict operations, which somehow decreases the read speed.
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
        """
        self.offsets: List[int] = []
        self.plain_index: List[str] = []

        with open(filename, "r+b") as f:
            try:
                # memory-map the file, size 0 means the whole file
                if not in_memory:
                    self._idx = mmap.mmap(f.fileno(), 0)
                else:
                    self._idx = f.read()

                num_offsets = struct.unpack("Q", self._idx[:8])[0]
                keys_offsets: List[int] = []
                for i in range(num_offsets):
                    offset = struct.unpack(
                        "Q", self._idx[(i + 1) * 8 : (i + 2) * 8]
                    )[0]

                    keys_offsets.append(offset)

                for i in range(num_offsets):
                    key = struct.unpack(
                        "Q", self._idx[(i + 1) * 8 : (i + 2) * 8]
                    )[0].decode("utf-8")

                    self.plain_index[key] = offset

            except ValueError as e:
                raise self.Error(f"Broken file format: {e}")

            self._start_pos = (num_offsets + 1) * 8

    def get(self, key: Hashable, default: Any = None) -> Optional[Any]:
        """
        :param key: an id at plain index to get an offset
            Key to search    
        :param default
            Default value to return if key is not found in index  
        """
        _id = self.id_mapping.get(key)
        if _id is None:
            return default

        offset = self.offsets[_id]

        if _id + 1 < len(self.offsets):
            binary_data = self._idx[
                self._start_pos
                + offset : self._start_pos
                + self.offsets[_id + 1]
            ]
        else:
            binary_data = self._idx[self._start_pos + offset :]

        try:
            binary_data = zlib.decompress(binary_data)
            if self.converter:
                binary_data = self.converter(binary_data)

            return binary_data
        except ValueError:
            return default

    @classmethod
    def create(
        cls,
        iterable: Union[Iterable, Generator],
        *,
        key: Callable[[Hashable], str] = lambda x: x[0],
        encoder: Callable[[Any], bytes] = lambda e: e.encode("utf-8"),
    ) -> Optional[str]:
        """
        Given an iterable of anything
        creates BinaryIndex for it.

        :param iterable: any 

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

        num_offsets = 0
        offset = 0
        keys_offset = 0
        for entry in iterable:
            num_offsets += 1
            data_offset_fp.write(
                struct.pack("Q", offset)
            )
            keys_offset_fp.write(
                struct.pack("Q", keys_offset)
            )

            try:
                entry: bytes = encoder(entry)
                key: str = key(entry)
            except Exception as e:
                raise cls.Error(
                    f"Couldn't convert {entry} to bytes"
                )

            binary_data = zlib.compress(entry)
            binary_key = zlib.compress(key.encode("utf-8"))

            data_fp.write(binary_data)
            keys_fp.write(binary_key)

            offset += len(binary_data)
            keys_offset += len(binary_key)

        data_offset_fp.write(
            struct.pack("Q", offset)
        )
        keys_offset_fp.write(
            struct.pack("Q", offset)
        )

        data_offset_fp.close()
        data_fp.close()
        keys_offset_fp.close()
        keys_fp.close()

        final_fp = tempfile.NamedTemporaryFile(
            suffix=".full.data", delete=False, mode="w+b"
        )
        final_fp.write(struct.pack("Q", num_offsets))

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

