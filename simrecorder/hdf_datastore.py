import os

import numpy as np
import h5py

from simrecorder.datastore import DataStore


class HDF5DataStore(DataStore):
    """
    This is a hd5 datastore. Currently, NOT threadsafe
    """

    def __init__(self, data_file_pth):
        assert h5py.version.hdf5_version_tuple >= (1,9,178), "SWMR requires HDF5 version >= 1.9.178 "
        "If you have libhdf5 version >= 1.10 but get this error, try installing h5py from source"
        "See: http://docs.h5py.org/en/latest/build.html#source-installation"

        if not os.path.exists(data_file_pth):
            self.f = h5py.File(data_file_pth, 'w', libver='latest')
        else:
            self.f = h5py.File(data_file_pth, 'r', libver='latest')
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
                d.resize(d.shape[0]+1, axis=0)
                d[-1:, ...] = obj
                d.flush()
            else:
                self.f.create_dataset(key, data=obj[None, ...], compression="lzf", maxshape=(None, *obj.shape))
        else:
            self.f.create_dataset("{}/{}".format(key, self.i), data=obj)
            self.i += 1

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
