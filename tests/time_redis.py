import os
import shutil

import numpy as np

from simrecorder import Recorder, RedisDataStore, RedisServer, Serialization
from tests import Timer, get_size


def main():
    read_only = False

    data_dir = os.path.expanduser('~/output/tmp/redis-test')
    os.makedirs(data_dir, exist_ok=True)
    key = 'train.what'
    bs = 10
    sts = 500 * 20
    ns = 200
    n_arrays = 100
    # serialization = Serialization.PYARROW
    serialization = Serialization.PICKLE

    if not read_only:
        arrays = np.random.rand(n_arrays, bs, sts, ns)

        if os.path.exists(data_dir):
            print("Removing existing dir")
            shutil.rmtree(data_dir)
        os.makedirs(data_dir, exist_ok=True)

        with RedisServer(data_directory=data_dir, serialization=serialization):
            ## WRITE
            redis_datastore = RedisDataStore(server_host='localhost')
            recorder = Recorder(redis_datastore)

            with Timer() as wt:
                write_times = []
                for i in range(n_arrays):
                    array = arrays[i]
                    with Timer() as st:
                        recorder.record(key, array)
                    print("%d: Storing took %.2fs" % (i, st.difftime))
                    write_times.append(st.difftime)
                print("Mean write time was %.4fs (+/- %.4f)" % (np.mean(write_times), np.std(write_times)))
                recorder.close()
            print("Total write time was %.2fs" % wt.difftime)
            ## END WRITE

        print("Dir size after write is %d MiB" % (int(get_size(data_dir)) / 1024 / 1024))

    ## READ
    with RedisServer(data_directory=data_dir, serialization=serialization):
        redis_datastore = RedisDataStore(server_host='localhost', data_directory=data_dir, serialization=serialization)
        recorder = Recorder(redis_datastore)

    with Timer() as rt:
        l = recorder.get_all(key)
    print("Reading took %.2fs" % rt.difftime)

    with Timer() as rrt:
        l = np.array(l)
    print("Into array took %.2f" % rrt.difftime)

    print("Mean is", np.mean(l), l.shape)

    recorder.close()
    ## END READ


if __name__ == "__main__":
    main()
