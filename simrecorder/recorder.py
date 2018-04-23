class Recorder:
    def __init__(self, *datastores):
        """
        Initialize Recorder with list of datastores
        :param datastores:
        """
        self.datastores = datastores
        for datastore in self.datastores:
            datastore.connect()

    def set(self, key, val, datastore=None):
        """
        Set a key to a particular value
        :param key: a string
        :param val: any object
        :param datastore: (optional) Specify which datastore to use to store value
        :return:
        """
        datastores = self.datastores
        if datastore is not None:
            datastores = [datastore]

        for datastore in datastores:
            datastore.set(key, val)

    def get(self, key, datastore=None):
        """
        Retrieve the value of key
        :param key:
        :param datastore:
        :return:
        """
        if datastore is not None:
            return datastore.get(key)
        else:
            return self.datastores[0].get(key)

    def record(self, key, val, datastore=None):
        """
        Append the value `val` to a list under name `key`
        :param key:
        :param val:
        :param datastore:
        :return:
        """
        datastores = self.datastores
        if datastore is not None:
            datastores = [datastore]

        for datastore in datastores:
            datastore.append(key, val)

    def get_all(self, key, datastore=None):
        """
        Get the list stored under key
        :param key:
        :param datastore:
        :return:
        """
        if datastore is not None:
            return datastore.get_all(key)
        else:
            return self.datastores[0].get_all(key)

    def close(self):
        """
        Close all datastores in the recorder.
        :return:
        """
        for datastore in self.datastores:
            datastore.close()
