__author__ = "Anand Subramoney"
__version__ = "0.9.1"

from .recorder import Recorder
from .datastores import InMemoryDataStore, RedisDataStore

__all__ = ['Recorder', 'InMemoryDataStore', 'RedisDataStore']

REDIS_PORT = 65535
