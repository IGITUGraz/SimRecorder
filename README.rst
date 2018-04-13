SimRecorder
===========

SimRecorder is an interface for recording simulation data and optionally persisting it in a backed. Currently two
different backends are supported -- `redis <https://redis.io>`_ and `hdf5 <https://support.hdfgroup.org/HDF5/>`_

Installation
++++++++++++

.. code:: bash

    pip install https://github.com/IGITUGraz/SimRecorder/archive/master.zip

By default only support for HDF5 is installed. To install redis support, clone the repository and run

.. code:: bash

    pip install -r requirements.redis.txt && pip install .

You can test if HDF5 and/or redis support was installed successfully by running the scripts in the ``tests`` directory.

Requirements
++++++++++++

HDF5 backend
------------

libhdf5 needs to be installed in the system using:

.. code:: bash

    sudo apt-get install libhdf5

Redis backend
-------------

All required packages (including redis) are installed through pip as dependencies of this package.


Quickstart
++++++++++

The library consists of a single ``Recorder`` interface that can be initialized to use different backends by passing in an
appropriate ``DataStore`` object.

First import the datastores you want to use

.. code:: python

    from simrecorder import Recorder, InMemoryDataStore, RedisDataStore, HDF5DataStore

Then initialize all the datastores you want (Yes, you can have more than one!). 

The ``InMemoryDataStore`` stores all data in memory

.. code:: python

    in_memory_datastore = InMemoryDataStore()

The ``HDF5Datastore`` stores all data in the given HDF5 file. If you use a file that already exists, it opens the file in
read-only mode.

.. code:: python

    hdf5_datastore = HDF5DataStore('~/output/data.h5')

The ``HDF5Datastore`` doesn't support distributed simulations yet, unless you have a single writer thread that handles all interaction
with the hdf5 file.

The ``RedisDataStore`` stores all data in redis (persisted in the given data_directory). Currently, you cannot have more
than one ``RedisDatastore`` being used per host.

For distributed simulations, you need to pass in the appropriate ``server_host`` of the main/master node in the code for
worker simulations running in the worker nodes/host.

.. code:: python

    redis_datastore = RedisDataStore(server_host='localhost', data_directory='~/output')


Then initialize the recorder with the datastore(s) you want to use 

.. code:: python

    # To use only in-memory datastore
    recorder = Recorder(in_memory_datastore)

    # To use only the hdf5 datastore
    recorder = Recorder(hdf5_datastore)

    # To use all
    recorder = Recorder(in_memory_datastore, redis_datastore, hdf5_datastore)


Then in your simulation, record the values you want. For each type of value, pass in a key. By default, every time you
use the same key, the value is appended to a list-like datastructure (in the underlying datastore)

Your keys can be any arbitrary string. Use '/' for efficient use of deeper hierarchies in HDF5 (For other datastores, it
makes no difference)

.. code:: python

    # This appends some_value to a list with key 'a/b'
    recorder.record('a/b', some_value1)
    recorder.record('a/b', some_value2)
    # This appends some_value to a list with key 'a/c'
    recorder.record('a/c', some_value2)


After the simulation is done, retrieve the values using ``recorder.get``, which returns a list of values. Note that if you
used the ``HDF5Datastore``, you might get ``HDFView`` objects that you can either pass in directly to most NumPy functions, 
or convert it to NumPy arrays first before use. 

The ``HDFView`` objects also allows you to work with larger-than-memory arrays, if you use only slices of the arrays.

.. code:: python

    # This gives you a list of values your recorded [some_value1, some_value2] (Retrieved from the first datastore)
    recorder.get_all('a')
    # You can also re-intialize recorder with the same parameters in other scripts and access the keys

You can also close the recorder after writing, and open it later for reading.

Remember to close the recorder after all reading/writing is done. This flushes data and closes the connection (where
applicable)

.. code:: python

    ## After everything
    recorder.close()

Tests
+++++

To make sure all the datastores work, run:

.. code:: bash

    python tests/test_datastores.py

To test the performance of the hdf5 and redis datastores, you can use the ``tests/time_*``. You can tune the size of 
the numpy array to reflect your use case. The default values are quite large -- for instance with the default values,
the resulting hdf5 file is about 4GB.

Backends
++++++++

* For storing large numpy arrays, use the HDF5 backend. 
* Redis backend is extremely fast for both reading and writing, as long as you're not storing large (>20MB) NumPy arrays
