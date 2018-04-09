import os

import numpy as np
import h5py
import h5py_cache

from simrecorder.datastore import DataStore


class HDF5DataStore(DataStore):
    """
    This is a hd5 datastore. Currently, NOT threadsafe
    """

    def __init__(self, data_file_pth):
        assert h5py.version.hdf5_version_tuple >= (1, 9, 178), "SWMR requires HDF5 version >= 1.9.178 "
        "If you have libhdf5 version >= 1.10 but get this error, try installing h5py from source"
        "See: http://docs.h5py.org/en/latest/build.html#source-installation"

        if not os.path.exists(data_file_pth):
            self.f = h5py.File(data_file_pth, 'w', libver='latest')
        else:
            # self.f = h5py.File(data_file_pth, 'r', libver='latest')
            self.f = h5py_cache.File(data_file_pth, 'r', chunk_cache_mem_size=20 * 1024 ** 3, libver='latest')  ## 20 GB
        self.i = 0

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
                d[-1:, ...] = obj
                d.flush()
            else:
                self.f.create_dataset(key, data=obj[None, ...], compression="lzf", maxshape=(None, *obj.shape),
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
        desired_chunk_size_bytes = 1024 ** 2  # 8K # 1024**2  # 1MB
        element_size_bytes = 4
        # Assuming storage of 32-bit floats
        if np.prod(obj.shape) * element_size_bytes <= desired_chunk_size_bytes:
            return obj.shape
        else:
            ndim = len(obj.shape)
            total_elements = desired_chunk_size_bytes / element_size_bytes
            # elements_per_dim = np.power(total_elements, 1 / ndim)
            # print('elements_per_dim', elements_per_dim)
            cum_el = total_elements
            # print('cum_el', cum_el)
            last_el_s = obj.shape[-1]
            cum_el /= last_el_s
            shape = [1]
            # print('cum_el', cum_el)
            for i in range(ndim - 1):
                # print(i, np.power(cum_el, 1 / (ndim - i - 1)))
                s = np.minimum(obj.shape[i], np.power(cum_el, 1 / (ndim - i - 1)))
                s = np.floor(s)
                cum_el /= s
                shape.append(int(s))
            shape.append(int(last_el_s))
            # print('cum_el ', cum_el)
            # print("shape ", shape)
            return tuple(shape)

    def get_all(self, key):
        d = self.f.get(key)
        if d is not None:
            if isinstance(d, h5py.Dataset):
                return d
            else:
                return list(
                    map(lambda x: x[1],
                        sorted(d.items(), key=lambda x: int(x[0]))
                        )
                )

    def close(self):
        self.f.close()

    def enable_swmr(self):
        """
        This should be done only after all datasets have been created. You cannot add new groups after this is done.
        :return:
        """
        self.f.swmr_mode = True
