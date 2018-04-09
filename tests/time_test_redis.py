import os
import time

import numpy as np

from simrecorder import Recorder, RedisDataStore, Serialization


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

    data_dir = os.path.expanduser('~/output/tmp/redis-test')
    os.makedirs(data_dir, exist_ok=True)
    key = 'train.what'
    serialization = Serialization.PYARROW
    # serialization=Serialization.PICKLE)

    ## WRITE
    redis_datastore = RedisDataStore(server_host='localhost', data_directory=data_dir, serialization=serialization)
    recorder = Recorder(redis_datastore)

    for i in range(n_arrays):
        array = arrays[i]
        with Timer() as st:
            recorder.record(key, array)
        print("Storing took %.2fs" % st.difftime)

    recorder.close()
    ## END WRITE

    ## READ
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
    from ipdb import launch_ipdb_on_exception

    with launch_ipdb_on_exception():
        main()
