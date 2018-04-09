import pickle
from enum import Enum
from multiprocessing import Pool

import lz4.frame
import pyarrow


class DataStore:
    """
    Interface for datastore. Any DataStore implementation must inherit from this.
    """

    def connect(self):
        return self

    def set(self, key, dict_obj):
        pass

    def get(self, key):
        pass

    def append(self, key, dict_obj):
        pass

    def get_all(self, key):
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

    def set(self, key, dict_obj):
        self.data[key] = dict_obj

    def get(self, key):
        return self.data.get(key)

    def append(self, key, dict_obj):
        self.data.setdefault(key, []).append(dict_obj)

    def get_all(self, key):
        return self.data.get(key, [])


Serialization = Enum('Serialization', ['PICKLE', 'PYARROW'])


class SerializationMixin:
    ## Private methods
    @staticmethod
    def _pickle_serialize(obj):
        return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

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
            return lz4.frame.compress(bstring)
        return bstring

    def _decompress(self, bstring):
        if self.use_compression:
            return lz4.frame.decompress(bstring)
        return bstring
