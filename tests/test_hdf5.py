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
    n_arrays = 10

    arrays = np.random.rand(n_arrays, 10, 500, 20, 600)

    data_dir = os.path.expanduser('~/output/tmp/hdf5-test')
    os.makedirs(data_dir, exist_ok=True)
    file_pth = os.path.join(data_dir, 'data.h5')
    if os.path.exists(file_pth):
        os.remove(file_pth)

    ## WRITE
    hdf5_datastore = HDF5DataStore(file_pth)
    recorder = Recorder(hdf5_datastore)

    for i in range(n_arrays):
        array = arrays[i]
        with Timer() as st:
            recorder.record('train/what', array)
        print("Storing took %.2fs" % st.difftime)
    recorder.close()
    ## END WRITE

    ## READ
    hdf5_datastore = HDF5DataStore(file_pth)
    recorder = Recorder(hdf5_datastore)

    with Timer() as rt:
        l = recorder.get('train/what')
    print("Reading took %.2fs" % rt.difftime)
    with Timer() as rrt:
        l = np.array(list(l))
    print("Into array took %.2f" % rrt.difftime)

    recorder.close()
    ## END READ


if __name__ == "__main__":
    main()
