Sim Recorder
============

Installation
++++++++++++

.. code:: bash

    pip install https://github.com/IGITUGraz/SimRecorder/archive/master.zip


Usage
+++++

.. code:: python

    from simrecorder import Recorder, InMemoryDataStore, RedisDataStore

    ## First initialize all the datastores you want (Yes, you can have more than one!)
    # The InMemoryDataStore stores all data in memory
    in_memory_datastore = InMemoryDataStore()

    # The RedisDataStore stores all data in redis
    redis_datastore = RedisDataStore(server_host='localhost', data_directory='~/output')

    # To use only in-memory datastore
    recorder = Recorder(in_memory_datastore)

    # To use both
    recorder = Recorder(in_memory_datastore, redis_datastore)

    ## Then in your simulation
    # This appends some_value to a list with key 'a'
    recorder.record('a', some_value1)
    recorder.record('a', some_value2)

    ## After the simulation is done, retrieve the values
    # This gives you a list of values your recorded [some_value1, some_value2]
    recorder.get('a')
    # You can also re-intialize recorder with the same parameters in other scripts and access the keys

    ## After everything
    recorder.close()

NOTE: All data (including primitive types) are stored as python pickles. If you directly access the data from redis, 
you should unpickle the values you get back from redis.
