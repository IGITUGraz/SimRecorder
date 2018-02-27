class Recorder:
    def __init__(self, *datastores):
        self.datastores = datastores
        for datastore in self.datastores:
            datastore.connect()

    def record(self, key, val, datastore=None):
        datastores = self.datastores
        if datastore is not None:
            datastores = [datastore]

        for datastore in datastores:
            if not datastore.get(key):
                datastore.initarr(key)
            datastore.append(key, val)

    def get(self, key, datastore=None):
        if datastore is not None:
            return datastore.get(key)
        else:
            return self.datastores[0].get(key)

    def close(self):
        for datastore in self.datastores:
            datastore.close()
