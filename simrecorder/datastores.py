import pickle
from enum import Enum

import pyarrow
import zlib
from rediscontroller import start_redis, stop_redis, is_redis_running
from rejson import Client, Path
from multiprocessing import Pool

REDIS_PORT = 65535


class DataStore:
    """
    Interface for datastore. Any DataStore implementation must inherit from this.
    """

    def connect(self):
        return self

    def store(self, key, dict_obj):
        pass

    def initarr(self, key):
        pass

    def append(self, key, dict_obj):
        pass

    def get(self, key):
        pass

    def close(self):
        pass


class InMemoryDataStore(DataStore):
    """
    Simple datastore that stores everything in memory
    """

    def __init__(self):
        self.data = {}

    def connect(self):
        return self

    def store(self, key, dict_obj):
        self.data[key] = dict_obj

    def initarr(self, key):
        self.data[key] = []

    def append(self, key, dict_obj):
        self.data[key].append(dict_obj)

    def get(self, key):
        return self.data.get(key, [])


Serialization = Enum('Serialization', ['PICKLE', 'PYARROW'])


class RedisDataStore(DataStore):
    """
    A datastore that persists all data to redis with the provided parameters
    """

    def __init__(self, server_host, data_directory, serialization=Serialization.PICKLE,
                 use_multiprocess_deserialization=False, use_compression=True):
        # , custom_json_encoder_cls=None, custom_json_decoder_cls=None):
        # Either both are None or both are not
        # assert not ((custom_json_encoder_cls is None) ^ (custom_json_decoder_cls is None))
        # self.custom_json_encoder_cls = custom_json_encoder_cls
        # self.custom_json_decoder_cls = custom_json_decoder_cls
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

        self.config = dict(server_host=server_host, data_directory=data_directory, serialization=str(serialization),
                           use_multiprocess_deserialization=use_multiprocess_deserialization,
                           use_compression=use_compression)

    @staticmethod
    def _pickle_serialize(obj):
        return pickle.dumps(obj)

    @staticmethod
    def _pickle_deserialize(bstring):
        return pickle.loads(bstring)

    @staticmethod
    def _pyarrow_serialize(obj):
        return pyarrow.serialize(obj).to_buffer().to_pybytes()

    @staticmethod
    def _pyarrow_deserialize(bstring):
        return pyarrow.deserialize(pyarrow.frombuffer(bstring))

    def _singleprocess_deserialize_list(self, results):
        return [self._deserialize(self._decompress(r)) for r in results]

    def _multiprocess_deserialize_list(self, results):
        with Pool() as p:
            return p.map(self._deserialize, p.map(self._decompress, results))

    def _compress(self, bstring):
        if self.use_compression:
            return zlib.compress(bstring)
        return bstring

    def _decompress(self, bstring):
        if self.use_compression:
            return zlib.decompress(bstring)
        return bstring

    def connect(self):
        if is_redis_running():
            raise RuntimeError(
                "Redis is already running (maybe from a previous experiment?). Did you forget to change the port?")

        start_redis(data_directory=self.data_directory)

        # if self.custom_json_encoder_cls is not None and self.custom_json_decoder_cls is not None:
        #     self.rj = Client(host=self.server_host, port=REDIS_PORT, encoder=self.custom_json_encoder_cls(),
        #                      decoder=self.custom_json_decoder_cls())  # , decode_responses=True)
        # else:
        self.rj = Client(host=self.server_host, port=REDIS_PORT)  # , decode_responses=True)
        self.store('config', self.config)
        return self

    def store(self, key, dict_obj):
        self.rj.jsonset(key, Path.rootPath(), dict_obj)

    def append(self, key, obj):
        serialized_obj = self._compress(self._serialize(obj))
        self.rj.rpush(key, serialized_obj)

    def get(self, key):
        if self.rj.type(key) == b'list':
            results = self.rj.lrange(key, 0, -1)
            return self._deserialize_list(results)
        # return self.rj.get(key)

    def close(self):
        stop_redis(redis_host=self.server_host)
