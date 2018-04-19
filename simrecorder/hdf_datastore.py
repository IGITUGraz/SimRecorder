import os

import h5py
import h5py_cache
import numpy as np

from simrecorder.datastore import DataStore


class HDF5DataStore(DataStore):
    """
    This is a hd5 datastore. Currently, NOT threadsafe
    """

    def __init__(self,
                 data_file_pth,
                 chunk_cache_mem_size_bytes=20 * 1024**3,
                 desired_chunk_size_bytes=0.1 * 1024**2,
                 compression='lzf'):
        """

        :param data_file_pth: Path to the hdf5 file
        :param chunk_cache_mem_size_bytes: HDF5 chunk cache size. Larger the better. Default is 20GiB
        :param desired_chunk_size_bytes: Chunk size for individual chunks. h5py docs recommends keeping this between
            10 KiB and 1 MiB. Default is 0.1 MiB. Pass in -1 to switch to h5py automagic chunk size.
        """
        self.desired_chunk_size_bytes = desired_chunk_size_bytes
        if not os.path.exists(data_file_pth):
            self.f = h5py.File(data_file_pth, 'w', libver='latest')
        else:
            # self.f = h5py.File(data_file_pth, 'r', libver='latest')
            self.f = h5py_cache.File(
                data_file_pth,
                'r',
                chunk_cache_mem_size=chunk_cache_mem_size_bytes,
                libver='latest',
                w0=0.1,
                n_cache_chunks=int(chunk_cache_mem_size_bytes / desired_chunk_size_bytes))
        self.i = 0
        self.is_swmr_hdf_version = h5py.version.hdf5_version_tuple >= (1, 9, 178)
        self.compression = compression

    def set(self, key, dict_obj):
        for k, v in dict_obj.items():
            self.f.create_dataset("{}/{}".format(key, k), data=v)

    def get(self, key):
        return self.f.get(key)

    def append(self, key, obj):
        if isinstance(obj, np.ndarray):
            d = self.f.get(key)
            if d is not None:
                assert isinstance(d, h5py.Dataset)
                # https://stackoverflow.com/a/25656175
                d.resize(d.shape[0] + 1, axis=0)
                # d[-1:, ...] = obj
                d[-1, ...] = obj
                if self.is_swmr_hdf_version:
                    d.flush()
            else:
                self.f.create_dataset(
                    key,
                    data=obj[None, ...],
                    compression=self.compression,
                    maxshape=(None, *obj.shape),
                    chunks=self._get_chunk_size(obj))
        else:
            self.f.create_dataset("{}/{}".format(key, self.i), data=obj)
            self.i += 1

    def _get_chunk_size(self, obj):
        """
        Tries to optimize the chunk size (assuming 32-bit floats used) so that the chunk size is close to 1MB. The last
        dimension size is maintained without change.
        :param obj:
        :return:
        """
        ## Makes sure chunk size is always 1MB!
        desired_chunk_size_bytes = self.desired_chunk_size_bytes    # 8K # 1024**2  # 1MB
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
            if isinstance(d, h5py.Dataset):
                return d
            else:
                return list(map(lambda x: x[1], sorted(d.items(), key=lambda x: int(x[0]))))

    def close(self):
        self.f.close()

    def enable_swmr(self):
        """
        This should be done only after all datasets have been created. You cannot add new groups after this is done.
        :return:
        """
        assert self.is_swmr_hdf_version, "SWMR requires HDF5 version >= 1.9.178 but is %s" % h5py.version.hdf5_version_tuple
        "If you have libhdf5 version >= 1.10 but get this error, try installing h5py from source"
        "See: http://docs.h5py.org/en/latest/build.html#source-installation"
        self.f.swmr_mode = True
