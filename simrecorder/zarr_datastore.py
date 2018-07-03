import os
from enum import Enum

import numpy as np

from simrecorder.datastore import DataStore

DatastoreType = Enum('DatastoreType', ['LMDB', 'DIRECTORY'])
CompressionType = Enum('CompressionType', ['BLOSC', 'LZMA'])


class ZarrDataStore(DataStore):
    """
    This is a zarr datastore. Uses lmdb underneath to store the data.
    """

    def __init__(self, data_dir_pth, desired_chunk_size_bytes=1. * 1024 ** 2, datastore_type=DatastoreType.LMDB, compression_type=CompressionType.BLOSC):
        """
        :param data_dir_pth: Path to the zarr lmdb file
        :param desired_chunk_size_bytes: The size (in bytes) of chunk each array is split into
        :param datastore_type: LMDB uses the lmdb database which needs to be installed on the system. If not available, use DIRECTORY type, which uses os filesystem
        :param compression_type: BLOSC uses the blosc library through numcodecs, but requires the blosc library to be installed on the system, or have a compatible system where blosc can be automatically installed when installing numcodes. If blosc is not available, use LZMA, which uses the python built-in compression library LZMA.
        """

        import zarr

        self.zarr = zarr

        self.datastore_type = datastore_type
        if datastore_type == DatastoreType.LMDB:
            self.store = zarr.LMDBStore(data_dir_pth)
        elif datastore_type == DatastoreType.DIRECTORY:
            self.store = zarr.DirectoryStore(data_dir_pth)
        else:
            raise RuntimeError('Unknown datastore type: {}'.format(datastore_type))

        if compression_type == CompressionType.BLOSC:
            from numcodecs import Blosc
            self.compressor = Blosc(cname='blosclz', clevel=9, shuffle=Blosc.BITSHUFFLE)
        elif compression_type == CompressionType.LZMA:
            # import lzma
            # lzma_filters = [dict(id=lzma.FILTER_DELTA, dist=4), dict(id=lzma.FILTER_LZMA2, preset=1)]
            from numcodecs import LZMA
            self.compressor = LZMA()

        self.desired_chunk_size_bytes = desired_chunk_size_bytes

        if not os.path.exists(data_dir_pth):
            self.f = zarr.group(store=self.store, overwrite=True)
        else:
            self.f = zarr.group(store=self.store, overwrite=False)

        self.i = 0

    def set(self, key, value):
        d = self.f.get(key)
        if d is None:
            self.f.create_dataset(key, data=value)
        else:
            self.f.create_dataset(key, data=value, overwrite=True)

    def get(self, key):
        return self.f.get(key)

    def append(self, key, obj):
        if isinstance(obj, np.ndarray):
            d = self.f.get(key)
            if d is not None:
                assert isinstance(d, self.zarr.core.Array)
                # https://stackoverflow.com/a/25656175
                d.resize(d.shape[0] + 1, *d.shape[1:])
                d[-1, ...] = obj
                if self.datastore_type == DatastoreType.LMDB:
                    self.store.flush()
            else:
                self.f.create_dataset(
                    key, data=obj[None, ...], compressor=self.compressor, chunks=self._get_chunk_size(obj))
        else:
            self.f.create_dataset("{}/{}".format(key, self.i), data=obj)
            self.i += 1

    def _get_chunk_size(self, obj):
        """
        Tries to optimize the chunk size (assuming 32-bit floats used) so that the chunk size is close to
        `desired_chunk_size_bytes`. The last dimension size is maintained without change.
        :param obj:
        :return:
        """
        ## Makes sure chunk size is always `desired_chunk_size_bytes`!
        desired_chunk_size_bytes = self.desired_chunk_size_bytes
        if desired_chunk_size_bytes <= 0:
            # Switch to h5py's automagic chunk size calculation
            return True

        element_size_bytes = 4
        # Assuming storage of 32-bit floats
        if np.prod(obj.shape) * element_size_bytes <= desired_chunk_size_bytes:
            return tuple([1] + list(obj.shape))
        else:
            ndim = len(obj.shape)
            total_elements = desired_chunk_size_bytes / element_size_bytes
            cum_el = total_elements
            last_el_s = obj.shape[-1]
            cum_el /= last_el_s
            shape = [1]
            for i in range(ndim - 1):
                s = np.minimum(obj.shape[i], np.power(cum_el, 1 / (ndim - i - 1)))
                s = np.floor(s)
                cum_el /= s
                shape.append(int(s))
            shape.append(int(last_el_s))
            assert len(shape) == len(obj.shape) + 1
            return tuple(shape)

    def get_all(self, key):
        d = self.f.get(key)
        if d is not None:
            if isinstance(d, self.zarr.core.Array):
                return d
            else:
                return list(map(lambda x: x[1], sorted(d.items(), key=lambda x: int(x[0]))))

    def close(self):
        if self.datastore_type == DatastoreType.LMDB:
            self.store.close()
