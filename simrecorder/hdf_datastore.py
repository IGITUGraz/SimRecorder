import os

import numpy as np
import h5py

from simrecorder.datastore import DataStore


class HDF5DataStore(DataStore):
    """
    This is a hd5 datastore. Currently, NOT threadsafe
    """

    def __init__(self, data_file_pth):
        if not os.path.exists(data_file_pth):
            self.f = h5py.File(data_file_pth, 'w')
        else:
            self.f = h5py.File(data_file_pth, 'r')
        self.i = 0

    def connect(self):
        return self

    def store(self, key, dict_obj):
        for k, v in dict_obj.items():
            self.f.create_dataset("{}/{}".format(key, k), data=v)

    def initarr(self, key):
        pass

    def append(self, key, dict_obj):
        if isinstance(dict_obj, np.ndarray):
            self.f.create_dataset("{}/{}".format(key, self.i), data=dict_obj, compression="gzip")
        else:
            self.f.create_dataset("{}/{}".format(key, self.i), data=dict_obj)
        self.i += 1

    def get(self, key):
        d = self.f.get(key)
        if d is not None:
            # return d.values()
            return list(
                map(lambda x: x[1],
                    sorted(d.items(), key=lambda x: int(x[0]))
                    )
            )

    def close(self):
        self.f.close()
