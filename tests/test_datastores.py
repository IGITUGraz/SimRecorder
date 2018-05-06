import os
import shutil
import unittest

import numpy as np

from simrecorder import (HDF5DataStore, InMemoryDataStore, Recorder,
                         RedisDataStore, RedisServer, ZarrDataStore)


class TestDatastores(unittest.TestCase):
    """
    Simple tests that record numpy 10 numpy arrays and read it back.
    """
    n_arrays = 10

    def setUp(self):
        self.arrays = np.random.rand(self.n_arrays, 10, 5, 2, 6)
        self.val = self.arrays[0]
        self.val1 = self.arrays[1]
        self.data_dir = os.path.expanduser('~/output/tmp/datastore-test')
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.key = 'train/what'

    def test_hdf5datastore_list(self):
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
        self.assertTrue((self.arrays == l).all())

        recorder.close()
        ## END READ

    def test_hdf5datastore_single_value(self):
        ## WRITE
        self.file_pth = os.path.join(self.data_dir, 'data.h5')
        hdf5_datastore = HDF5DataStore(self.file_pth)
        recorder = Recorder(hdf5_datastore)
        recorder.set(self.key, self.val1)
        recorder.set(self.key, self.val)
        recorder.close()
        ## END WRITE

        ## READ
        hdf5_datastore = HDF5DataStore(self.file_pth)
        recorder = Recorder(hdf5_datastore)

        l = recorder.get(self.key)
        l = np.array(l)
        self.assertTrue((self.val == l).all())

        recorder.close()
        ## END READ

    def test_inmemorydatastore_list(self):
        ## WRITE
        inmem_datastore = InMemoryDataStore()
        recorder = Recorder(inmem_datastore)

        for i in range(self.n_arrays):
            array = self.arrays[i]
            recorder.record(self.key, array)
        recorder.close()
        ## END WRITE

        ## READ
        # If the in-memory datastore is initialized, all data will be reset!
        recorder = Recorder(inmem_datastore)

        l = recorder.get_all(self.key)
        l = np.array(l)
        self.assertTrue((self.arrays == l).all())

        recorder.close()
        ## END READ

    def test_inmemorydatastore_single_value(self):
        ## WRITE
        inmem_datastore = InMemoryDataStore()
        recorder = Recorder(inmem_datastore)

        recorder.set(self.key, self.val1)
        recorder.set(self.key, self.val)
        recorder.close()
        ## END WRITE

        ## READ
        # If the in-memory datastore is initialized, all data will be reset!
        recorder = Recorder(inmem_datastore)

        l = recorder.get(self.key)
        l = np.array(l)
        self.assertTrue((self.val == l).all())

        recorder.close()
        ## END READ

    def test_redisdatastore_list(self):
        with RedisServer(data_directory=self.data_dir):
            ## WRITE
            redis_datastore = RedisDataStore(server_host='localhost')
            recorder = Recorder(redis_datastore)

            for i in range(self.n_arrays):
                array = self.arrays[i]
                recorder.record(self.key, array)
            recorder.close()
            ## END WRITE

        with RedisServer(data_directory=self.data_dir):
            ## READ
            redis_datastore = RedisDataStore(server_host='localhost')
            recorder = Recorder(redis_datastore)

            l = recorder.get_all(self.key)
            l = np.array(l)
            self.assertTrue((self.arrays == l).all())

            recorder.close()
            ## END READ

    def test_redisdatastore_single_value(self):
        with RedisServer(data_directory=self.data_dir):
            ## WRITE
            redis_datastore = RedisDataStore(server_host='localhost')
            recorder = Recorder(redis_datastore)

            recorder.set(self.key, self.val1)
            recorder.set(self.key, self.val)
            recorder.close()
            ## END WRITE

        with RedisServer(data_directory=self.data_dir):
            ## READ
            redis_datastore = RedisDataStore(server_host='localhost')
            recorder = Recorder(redis_datastore)

            l = recorder.get(self.key)
            l = np.array(l)
            self.assertTrue((self.val == l).all())

            recorder.close()
            ## END READ

    def test_zarrdatastore_list(self):
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
        self.assertTrue((self.arrays == l).all())

        recorder.close()
        ## END READ

    def test_zarrdatastore_single_value(self):
        ## WRITE
        assert not os.path.exists(os.path.join(self.data_dir, 'test.mdb'))
        zarr_datastore = ZarrDataStore(os.path.join(self.data_dir, 'test.mdb'))
        recorder = Recorder(zarr_datastore)

        recorder.set(self.key, self.val1)
        recorder.set(self.key, self.val)
        recorder.close()
        ## END WRITE

        ## READ
        zarr_datastore = ZarrDataStore(os.path.join(self.data_dir, 'test.mdb'))
        recorder = Recorder(zarr_datastore)

        l = recorder.get(self.key)
        l = np.array(l)
        self.assertTrue((self.val == l).all())

        recorder.close()
        ## END READ


if __name__ == "__main__":
    unittest.main()
