import os
import shutil
import unittest

import numpy as np

from simrecorder import (HDF5DataStore, InMemoryDataStore, Recorder,
                         RedisDataStore, ZarrDataStore)


class TestDatastores(unittest.TestCase):
    """
    Simple tests that record numpy 10 numpy arrays and read it back.
    """
    n_arrays = 10

    def setUp(self):
        self.arrays = np.random.rand(self.n_arrays, 10, 5, 2, 6)
        self.data_dir = os.path.expanduser('~/output/tmp/datastore-test')
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.key = 'train/what'

    def test_hdf5datastore(self):
        ## WRITE
        self.file_pth = os.path.join(self.data_dir, 'data.h5')
        hdf5_datastore = HDF5DataStore(self.file_pth)
        recorder = Recorder(hdf5_datastore)

        for i in range(self.n_arrays):
            array = self.arrays[i]
            recorder.record(self.key, array)
            # if i == 0:
            #     hdf5_datastore.enable_swmr()
        recorder.close()
        ## END WRITE

        ## READ
        hdf5_datastore = HDF5DataStore(self.file_pth)
        recorder = Recorder(hdf5_datastore)

        l = recorder.get_all(self.key)
        l = np.array(l)
        print("Mean is", np.mean(l), l.shape)

        recorder.close()
        ## END READ

    def test_inmemorydatastore(self):
        ## WRITE
        inmem_datastore = InMemoryDataStore()
        recorder = Recorder(inmem_datastore)

        for i in range(self.n_arrays):
            array = self.arrays[i]
            recorder.record(self.key, array)
        recorder.close()
        ## END WRITE

        ## READ
        inmem_datastore = InMemoryDataStore()
        recorder = Recorder(inmem_datastore)

        l = recorder.get_all(self.key)
        l = np.array(l)
        print("Mean is", np.mean(l), l.shape)

        recorder.close()
        ## END READ

    def test_redisdatastore(self):
        ## WRITE
        redis_datastore = RedisDataStore(server_host='localhost', data_directory=self.data_dir)
        recorder = Recorder(redis_datastore)

        for i in range(self.n_arrays):
            array = self.arrays[i]
            recorder.record(self.key, array)
        recorder.close()
        ## END WRITE

        ## READ
        redis_datastore = RedisDataStore(server_host='localhost', data_directory=self.data_dir)
        recorder = Recorder(redis_datastore)

        l = recorder.get_all(self.key)
        l = np.array(l)
        print("Mean is", np.mean(l), l.shape)

        recorder.close()
        ## END READ

    def test_zarrdatastore(self):
        ## WRITE
        assert not os.path.exists(os.path.join(self.data_dir, 'test.mdb'))
        zarr_datastore = ZarrDataStore(os.path.join(self.data_dir, 'test.mdb'))
        recorder = Recorder(zarr_datastore)

        for i in range(self.n_arrays):
            array = self.arrays[i]
            recorder.record(self.key, array)
        recorder.close()
        ## END WRITE

        ## READ
        zarr_datastore = ZarrDataStore(os.path.join(self.data_dir, 'test.mdb'))
        recorder = Recorder(zarr_datastore)

        l = recorder.get_all(self.key)
        l = np.array(l)
        print("Mean is", np.mean(l), l.shape)

        recorder.close()
        ## END READ


if __name__ == "__main__":
    unittest.main()
