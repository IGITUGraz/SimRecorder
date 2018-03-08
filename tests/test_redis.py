import os
import time
import numpy as np

from simrecorder import RedisDataStore, Serialization, Recorder


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
    n_arrays = 20

    arrays = np.random.rand(n_arrays, 10, 500, 20, 600)

    data_dir = '/tmp/redis-test'
    os.makedirs(data_dir, exist_ok=True)

    redis_datastore = RedisDataStore(server_host='localhost', data_directory=data_dir,
                                     serialization=Serialization.PICKLE)
    recorder = Recorder(redis_datastore)

    for i in range(n_arrays):
        array = arrays[i]
        with Timer() as st:
            recorder.record('train.what', array)
        print("Storing took %.2fs" % st.difftime)

    recorder.close()


if __name__ == "__main__":
    main()
