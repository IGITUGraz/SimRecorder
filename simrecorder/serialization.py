import pickle
from enum import Enum
from multiprocessing.pool import Pool

import lz4.frame
import pyarrow

Serialization = Enum('Serialization', ['PICKLE', 'PYARROW'])


class SerializationMixin:
    """
    Mixin to do serialization for a datastore if required. Supports ability to do serialization in a separate process.
    """
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
