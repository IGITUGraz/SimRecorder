from warnings import warn
import pickle

from rejson import Client, Path
from rediscontroller import start_redis, stop_redis, is_redis_running

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


class RedisDataStore(DataStore):
    """
    A datastore that persists all data to redis with the provided parameters
    """

    def __init__(self, server_host, data_directory):  # , custom_json_encoder_cls=None, custom_json_decoder_cls=None):
        # Either both are None or both are not
        # assert not ((custom_json_encoder_cls is None) ^ (custom_json_decoder_cls is None))
        # self.custom_json_encoder_cls = custom_json_encoder_cls
        # self.custom_json_decoder_cls = custom_json_decoder_cls
        self.server_host = server_host
        self.data_directory = data_directory
        self.rj = None

    def connect(self):
        if is_redis_running():
            raise RuntimeError("Redis is already running (maybe from a previous experiment?). Did you forget to change the port?")

        start_redis(data_directory=self.data_directory)

        # if self.custom_json_encoder_cls is not None and self.custom_json_decoder_cls is not None:
        #     self.rj = Client(host=self.server_host, port=REDIS_PORT, encoder=self.custom_json_encoder_cls(),
        #                      decoder=self.custom_json_decoder_cls())  # , decode_responses=True)
        # else:
        self.rj = Client(host=self.server_host, port=REDIS_PORT) # , decode_responses=True)
        return self

    # def store(self, key, dict_obj):
    #     self.rj.jsonset(key, Path.rootPath(), dict_obj)

    def append(self, key, obj):
        pickled_obj = pickle.dumps(obj)
        self.rj.rpush(key, pickled_obj)

    def get(self, key):
        if self.rj.type(key) == b'list':
            results = self.rj.lrange(key, 0, -1)
            return [pickle.loads(r) for r in results]
        # return self.rj.get(key)

    def close(self):
        stop_redis(redis_host=self.server_host)
