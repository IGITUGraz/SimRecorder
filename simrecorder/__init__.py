from .recorder import Recorder
from .datastore import InMemoryDataStore
from .hdf_datastore import HDF5DataStore

__all__ = ['Recorder', 'InMemoryDataStore', 'HDF5DataStore']

try:
    from .redis_datastore import RedisDataStore

    __all__.append('RedisDataStore')
except ImportError:
    pass
