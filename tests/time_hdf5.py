
import os

import numpy as np

from simrecorder import Recorder, HDF5DataStore
from tests import Timer


def main():
    read_only = False
    data_dir = os.path.expanduser('~/output/tmp/hdf5-test')
    file_pth = os.path.join(data_dir, 'data.h5')
    key = 'train/what'
    bs = 10
    sts = 500 * 20
    ns = 200
    n_arrays = 100
    chunk_size_mb = 0.1

    if not read_only:
        arrays = np.random.rand(n_arrays, bs, sts, ns)

        os.makedirs(data_dir, exist_ok=True)
        if os.path.exists(file_pth):
            print("Removing existing file")
            os.remove(file_pth)

        ## WRITE
        hdf5_datastore = HDF5DataStore(file_pth, desired_chunk_size_bytes=chunk_size_mb * 1024 ** 2)
        recorder = Recorder(hdf5_datastore)

        with Timer() as wt:
            write_times = []
            for i in range(n_arrays):
                array = arrays[i]
                with Timer() as st:
                    recorder.record(key, array)
                if i == 0:
                    hdf5_datastore.enable_swmr()
                print("%d: Storing took %.2fs" % (i, st.difftime))
                write_times.append(st.difftime)
            print("Mean write time was %.4fs (+/- %.4f)" % (np.mean(write_times), np.std(write_times)))
            recorder.close()
        print("Total write time was %.2fs" % wt.difftime)
        ## END WRITE

        print("File size after write is %d MiB" % (int(os.path.getsize(file_pth)) / 1024 / 1024))

    ## READ
    hdf5_datastore = HDF5DataStore(file_pth, desired_chunk_size_bytes=chunk_size_mb * 1024 ** 2)
    recorder = Recorder(hdf5_datastore)

    with Timer() as rt:
        l = recorder.get_all(key)
    print("Reading took %.2fs" % rt.difftime)

    read_times = []
    for i in range(20):
        b = np.random.randint(bs)
        st = np.random.randint(sts)
        with Timer() as rrt:
            ll = np.array(l[:(n_arrays // 2), b, st, :])
        print("Into sub-array took %.4fs" % rrt.difftime)
        read_times.append(rrt.difftime)
    print("Into sub-array mean readtime was %.4fs (+/- %.4f)" % (np.mean(read_times), np.std(read_times)))

    with Timer() as rrt:
        l = np.array(l)
    print("Into array (Total read time) was %.2fs" % rrt.difftime)

    print("Data mean is", np.mean(l), l.shape)

    recorder.close()
    ## END READ


if __name__ == "__main__":
    main()
