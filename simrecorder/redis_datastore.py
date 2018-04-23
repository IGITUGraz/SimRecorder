import redis
from rediscontroller import is_redis_running, start_redis, stop_redis

from simrecorder.datastore import DataStore
from simrecorder.serialization import Serialization, SerializationMixin

REDIS_PORT = 65535


class RedisDataStore(DataStore, SerializationMixin):
    """
    A datastore that persists all data to redis with the provided parameters
    """

    def __init__(self,
                 server_host,
                 data_directory,
                 serialization=Serialization.PICKLE,
                 use_multiprocess_deserialization=False,
                 use_compression=True):
        self.server_host = server_host
        self.data_directory = data_directory
        self.rj = None

        if serialization == Serialization.PICKLE:
            self._serialize = self._pickle_serialize
            self._deserialize = self._pickle_deserialize
        elif serialization == Serialization.PYARROW:
            self._serialize = self._pyarrow_serialize
            self._deserialize = self._pyarrow_deserialize

        if use_multiprocess_deserialization:
            self._deserialize_list = self._multiprocess_deserialize_list
        else:
            self._deserialize_list = self._singleprocess_deserialize_list

        self.use_compression = use_compression

        self.config = dict(
            server_host=server_host,
            data_directory=data_directory,
            serialization=str(serialization),
            use_multiprocess_deserialization=use_multiprocess_deserialization,
            use_compression=use_compression)

    def connect(self):
        if is_redis_running():
            raise RuntimeError(
                "Redis is already running (maybe from a previous experiment?). Did you forget to change the port?")

        start_redis(data_directory=self.data_directory)

        self.rj = redis.StrictRedis(host=self.server_host, port=REDIS_PORT)    # , decode_responses=True)
        self.set('config', self.config)
        return self

    def set(self, key, value):
        serialized_obj = self._compress(self._serialize(value))
        self.rj.set(key, serialized_obj)

    def get(self, key):
        val = self.rj.get(key)
        if val is not None:
            return self._deserialize(self._decompress(val))

    def append(self, key, obj):
        serialized_obj = self._compress(self._serialize(obj))
        self.rj.rpush(key, serialized_obj)

    def get_all(self, key):
        if self.rj.type(key) == b'list':
            results = self.rj.lrange(key, 0, -1)
            return self._deserialize_list(results)

    def close(self):
        stop_redis(redis_host=self.server_host)
