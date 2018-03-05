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
