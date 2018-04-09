import time

import numpy as np
import os

from simrecorder import Recorder, HDF5DataStore


class Timer:
    def __init__(self):
        self._startime = None
        self._endtime = None
        self.difftime = None

    def __enter__(self):
        self._startime = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._endtime = time.time()
        self.difftime = self._endtime - self._startime


def main():
    read_only = False
    data_dir = os.path.expanduser('~/output/tmp/hdf5-test')
    file_pth = os.path.join(data_dir, 'data.h5')
    key = 'train/what'
    bs = 10
    sts = 500 * 20
    ns = 200

    if not read_only:
        n_arrays = 10

        arrays = np.random.rand(n_arrays, bs, sts, ns)

        os.makedirs(data_dir, exist_ok=True)
        if os.path.exists(file_pth):
            os.remove(file_pth)

        ## WRITE
        hdf5_datastore = HDF5DataStore(file_pth)
        recorder = Recorder(hdf5_datastore)

        for i in range(n_arrays):
            array = arrays[i]
            with Timer() as st:
                recorder.record(key, array)
            if i == 0:
                hdf5_datastore.enable_swmr()
            print("Storing took %.2fs" % st.difftime)
        recorder.close()
        ## END WRITE

        print("File size after write is %d MiB" % (int(os.path.getsize(file_pth)) / 1024 / 1024))

    ## READ
    hdf5_datastore = HDF5DataStore(file_pth)
    recorder = Recorder(hdf5_datastore)

    with Timer() as rt:
        l = recorder.get_all(key)
    print("Reading took %.2fs" % rt.difftime)

    for i in range(10):
        b = np.random.randint(bs)
        st = np.random.randint(sts)
        with Timer() as rrt:
            ll = np.array(l[:5, b, st, :])
        print("Into sub-array took %.2f" % rrt.difftime)

    with Timer() as rrt:
        l = np.array(l)
    print("Into array took %.2f" % rrt.difftime)

    print("Mean is", np.mean(l), l.shape)

    recorder.close()
    ## END READ

    # os.remove(file_pth)


if __name__ == "__main__":
    from ipdb import launch_ipdb_on_exception

    with launch_ipdb_on_exception():
        main()
