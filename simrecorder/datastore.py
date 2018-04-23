class DataStore:
    """
    Interface for datastore. Any DataStore implementation must inherit from this.
    """

    def connect(self):
        """
        Connect to the instance of the datastore if necessary (e.g. with redis or other databases)
        :return:
        """
        return self

    def set(self, key, value):
        """
        Store a single value under key. Calling this repeatedly with the same key will overwrite the value
        :param key:
        :param value:
        :return:
        """
        pass

    def get(self, key):
        """
        Get the value stored under key. Returns None if key not found. Prefer :meth:`.get_all` for accessing lists
        of values
        :param key:
        :return:
        """
        pass

    def append(self, key, obj):
        """
        Append `obj` to a list under the key `key`. Every additional call will append obj to the key.

        :param key:
        :param obj:
        :return:
        """
        pass

    def get_all(self, key):
        """
        Get a list of values stored under key using :meth:`.append`. For some datastores, getting a list is a different
        call than getting a single value
        :param key:
        :return:
        """
        pass

    def close(self):
        """
        Do the appropriate shutdown sequence for the datastore.
        :return:
        """
        pass


class InMemoryDataStore(DataStore):
    """
    Simple datastore that stores everything in memory
    """

    def __init__(self):
        self.data = {}

    def connect(self):
        return self

    def set(self, key, value):
        self.data[key] = value

    def get(self, key):
        return self.data.get(key)

    def append(self, key, obj):
        self.data.setdefault(key, []).append(obj)

    def get_all(self, key):
        return self.data.get(key, [])
