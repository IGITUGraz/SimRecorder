from simrecorder.datastore import DataStore
from simrecorder.serialization import Serialization, SerializationMixin
import pickle

import logging

from numbers import Integral
import os

REDIS_PORT = 65535

logger = logging.getLogger('simrecorder.redis_datastore')


class RedisDataStore(DataStore, SerializationMixin):
    """
    A datastore that connects to a redis server and stores and retrieves data from the
    server. All configuration pertaining to the format of data stored in the database,
    is to be specified in the RedisServer instance to which this connects.

    :param server_host: The string identifying the host containing the redis server
    :param redis_port: The port number on which the redis port is active
    """

    def __init__(self, server_host, redis_port=REDIS_PORT):

        self.server_host = server_host
        self.redis_port = redis_port
        self.rj = None

        import redis
        self.rj = redis.StrictRedis(host=self.server_host, port=self.redis_port)  # , decode_responses=True)
        config_dict = self.get_config()['client']

        serialization = config_dict['serialization']
        use_multiprocess_deserialization = config_dict['use_multiprocess_deserialization']
        use_compression = config_dict['use_compression']

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
            redis_port=redis_port,
            serialization=str(serialization),
            use_multiprocess_deserialization=use_multiprocess_deserialization,
            use_compression=use_compression)

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

    def get_config(self):
        """
        Get's the client and server configuration from the database
        The config is stored using uncompressed pickle so that clients can read the
        config of any server independent of server configuration.
        """
        client_config_data = self.rj.get('client_config')
        server_config_data = self.rj.get('server_config')

        if client_config_data is not None:
            client_config_dict = pickle.loads(client_config_data)
            client_config_dict['serialization'] = getattr(Serialization, client_config_dict['serialization'])
            server_config_dict = pickle.loads(server_config_data)
        else:
            raise RuntimeError("The Redis server at host {} and port {} has not been"
                               " appropriately initialized. The client and server"
                               " configurations cannot be found. (Maybe you haven't"
                               " called the start() method on the server?)"
                               .format(self.server_host, self.redis_port))

        return dict(client=client_config_dict, server=server_config_dict)


class RedisServer:
    """
    This is a Redis server instance that starts a redis server with a particular
    configuration. The server can be started and stopped via it's start and stop
    methods, or it may be used as a context manager

    :param data_directory: The directory in which to start the redis server
    :param redis_port: Either an interger representing the port number on which the
        server operates, or the string 'random', in which case a random port is used
    :param config_file_path: The path of the config file to use to setup the
        server. If None, uses the default config file installed by the redis-
        installer package. Note that daemonize must be set to true for any config
        file that you may want to use.

    :param serialization: The serialization type used by :class:`RedisDataStore`
        instances to store data.
    :param use_multiprocess_deserialization: Whether the :class:`RedisDataStore`
        clients use multiprocessing to deserialize data from the database
    :param use_compression: `bool` value, whether the :class:`RedisDataStore`
        clients use compression when storing and retrieving data

    The parameters `serialization` `use_multiprocess_deserialization`
    `use_compression` are configuration options that affect the way the class
    :class:`.RedisDataStore` handles the data. These options are termed as the
    client configuration options. They are specified when creating the server as
    this is the only way they can be recorded and enforced consistently across
    clients. Any :class:`.RedisDataStore` instance which connects to the server
    will read the configuration of the server

    If the database is an existing database that is being appended to, then the
    client configuration specified by the arguments is IGNORED and the existing one
    in the database is used. This is to ensure the uniformity in the data presented
    """

    def __init__(self, data_directory, redis_port=REDIS_PORT, config_file_path=None,
                 serialization=Serialization.PICKLE,
                 use_multiprocess_deserialization=False,
                 use_compression=True):
        """
        """
        assert isinstance(redis_port, Integral) or \
            (isinstance(redis_port, str) and redis_port == 'random'), \
            ("redis_port should be either an interger representing the port number on which"
             " the server operates, or the string 'random'. It is currently {}".format(redis_port))
        assert os.path.isdir(data_directory), \
            "The data_directory: {} is not a valid directory".format(data_directory)

        self.data_directory = data_directory
        self._redis_port = redis_port
        self.config_file_path = config_file_path
        self.redis_port = None  # only assigned once the server is started
        self.rj = None

        self.server_config = dict(
            data_directory=data_directory,
            redis_port=redis_port,
            config_file_path=config_file_path)

        self.client_config = dict(
            serialization=str(serialization.name),
            use_multiprocess_deserialization=use_multiprocess_deserialization,
            use_compression=use_compression)

    def start(self):
        """
        Start the redis server
        """
        import redis
        from rediscontroller import is_redis_running, start_redis
        if not isinstance(self._redis_port, str):
            if is_redis_running(redis_port=self._redis_port):
                raise RuntimeError(
                    "Redis is already running at port {} (maybe from a previous experiment?). Did "
                    "you forget to change the port?".format(self._redis_port))

        self.redis_port = start_redis(data_directory=self.data_directory, redis_port=self._redis_port)
        self.rj = redis.StrictRedis(host='localhost', port=self.redis_port)  # , decode_responses=True)
        existing_client_config = self.rj.get('client_config')
        self.rj.set('server_config', pickle.dumps(self.server_config, protocol=0))
        if existing_client_config is None:
            self.rj.set('client_config', pickle.dumps(self.client_config, protocol=0))
        else:
            logger.warn('Found existing database in directory %s, ignoring specified'
                        ' client configuration', self.data_directory)

    def stop(self):
        """
        Stop the redis server
        """
        from rediscontroller import stop_redis
        stop_redis(redis_port=self.redis_port)
        self.redis_port = None
        self.rj = None

    def get_port(self):
        return self.redis_port

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()
