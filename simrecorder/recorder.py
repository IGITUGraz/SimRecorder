class Recorder:
    def __init__(self, *datastores):
        self.datastores = datastores
        for datastore in self.datastores:
            datastore.connect()

    def set(self, key, val, datastore=None):
        datastores = self.datastores
        if datastore is not None:
            datastores = [datastore]

        for datastore in datastores:
            datastore.set(key, val)

    def get(self, key, datastore=None):
        if datastore is not None:
            return datastore.get(key)
        else:
            return self.datastores[0].get(key)

    def record(self, key, val, datastore=None):
        datastores = self.datastores
        if datastore is not None:
            datastores = [datastore]

        for datastore in datastores:
            datastore.append(key, val)

    def get_all(self, key, datastore=None):
        if datastore is not None:
            return datastore.get_all(key)
        else:
            return self.datastores[0].get_all(key)

    def close(self):
        for datastore in self.datastores:
            datastore.close()
