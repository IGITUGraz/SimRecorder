.. image:: https://travis-ci.org/IGITUGraz/SimRecorder.svg?branch=master
    :target: https://travis-ci.org/IGITUGraz/SimRecorder
    
SimRecorder
===========

The goal of SimRecorder is to provide a simple and unified interface for recording and retrieving 
simulation data with transparent support for multiple backend storage formats. The library is optimized for storing 
large NumPy arrays, but can handle most datatypes. Currently three different storage backends are supported 
--  `zarr <https://zarr.readthedocs.io/en/stable/>`_, `hdf5 <https://support.hdfgroup.org/HDF5/>`_, and 
`redis <https://redis.io>`_


Installation
++++++++++++

.. code:: bash

    pip install https://github.com/IGITUGraz/SimRecorder/archive/master.zip

By default only support for zarr and HDF5 are installed. To install redis support, clone the repository and run

.. code:: bash

    pip install -r requirements.redis.txt && pip install .

You can test if various backends work correctly by running the scripts in the ``tests`` directory.

Requirements
++++++++++++

Zarr backend
------------

All required packages (including lmdb) are installed through pip as dependencies of this package.

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

The library consists of a single ``Recorder`` interface that can be initialized to use different backends by passing 
in an appropriate ``DataStore`` object.

1. First import the datastores you want to use

   .. code:: python

       from simrecorder import Recorder, InMemoryDataStore, RedisDataStore, HDF5DataStore, ZarrDatastore

2. Then initialize all the datastores you want (Yes, you can have more than one!). 

   The ``InMemoryDataStore`` stores all data in memory

   .. code:: python

       in_memory_datastore = InMemoryDataStore()

   The ``ZarrDatastore`` stores all data using the given path as the directory for the lmdb database files. 
   If you use a directory path that already exists, it opens the database in read-only mode.

   .. code:: python

       zarr_datastore = ZarrDataStore('~/output/data.mdb')

   The ``HDF5Datastore`` stores all data in the given HDF5 file. If you use a file that already exists, it opens the file in
   read-only mode.

   .. code:: python

       hdf5_datastore = HDF5DataStore('~/output/data.h5')

   The ``HDF5Datastore`` and ``ZarrDataStore`` don't support distributed simulations yet, unless you have a single writer 
   thread that handles all interaction with the hdf5 file.

   The ``RedisDataStore`` stores all data in redis (persisted in the given data_directory). Currently, you cannot have more
   than one ``RedisDatastore`` being used per host.

   For distributed simulations, you need to pass in the appropriate ``server_host`` of the main/master node in the code for
   worker simulations running in the worker nodes/host.

   .. code:: python

       redis_datastore = RedisDataStore(server_host='localhost', data_directory='~/output')


3. Then initialize the recorder with the datastore(s) you want to use 

   .. code:: python

       # To use only in-memory datastore
       recorder = Recorder(in_memory_datastore)

       # To use only the zarr datastore
       recorder = Recorder(zarr_datastore)

       # To use only the hdf5 datastore
       recorder = Recorder(hdf5_datastore)

       # To use more than one
       recorder = Recorder(in_memory_datastore, redis_datastore, hdf5_datastore)


4. In your simulation, record the values you want. For each type of value, pass in a key. By default, every time you use
   the same key, the value is appended to a list-like datastructure (in the underlying datastore)

   Your keys can be any arbitrary string. Use '/' for efficient use of deeper hierarchies in Zarr and HDF5 
   (For other datastores, it makes no difference)

   .. code:: python

       # This appends some_value to a list with key 'a/b'
       recorder.record('a/b', some_value1)
       recorder.record('a/b', some_value2)
       # This appends some_value to a list with key 'a/c'
       recorder.record('a/c', some_value2)

5. After the simulation is done, retrieve the values using ``recorder.get``, which returns a list of values. 
   
   Note that if you used the ``ZarrDatastore``, you will get ``zarr.core.Array`` objects that you can either pass in
   directly to most NumPy functions, or convert it to NumPy arrays first before use.  The ``zarr.core.Array`` objects
   also allow you to work with larger-than-memory arrays, if you use only slices of the arrays.

   The ``HDF5Datastore`` similarly returns ``HDFView`` objects that have similar properties as ``zarr.core.Array``.

   .. code:: python

       # This gives you a list of values your recorded [some_value1, some_value2] (Retrieved from the first datastore)
       recorder.get_all('a')
       # You can also re-intialize recorder with the same parameters in other scripts and access the keys

   You can also close the recorder after writing, and open it later for reading.

6. Remember to close the recorder after all reading/writing is done. This flushes data and closes the connection (where
   applicable)

   .. code:: python

       ## After everything
       recorder.close()

Tests
+++++

To make sure all the datastores work, run:

.. code:: bash

    python tests/test_datastores.py

To test the performance of the zarr, hdf5 and redis datastores, you can use the ``tests/time_*``. You can tune the size
of the numpy array to reflect your use case. The default values are quite large -- for instance with the default values,
the resulting hdf5 file is about 4GB.

Backends
++++++++

* The Zarr backend is the recommended backend if you are running simulations on a single node. It works well for large
  NumPy arrays as well.
* For distributed simulations running across multiple nodes, the redis backend should be used.
* Redis backend is extremely fast for both reading and writing, as long as you're not storing large (>20MB) NumPy arrays

Supercomputers
++++++++++++++

When using on supercomputers be advised that you may need to compile some dependencies from source. For example ``Blosc``, because of missing CPU optimizations.

Performance benchmarks
++++++++++++++++++++++

For one single run, on a NFS disk, Intel Xeon machine, with default parameters, for the specific 4-D array of size
100x10x10000x200 float32 values. For comparitive purposes only! You can run your own tests using scripts
``tests/time_*``.

====================  ====================  ===================  ==========================  ===================  ==================
   Backend            Total write time (s)  Mean write time (s)  Slicing mean read time (s)  Total read time (s)  Size on disk (GB)
--------------------  --------------------  -------------------  --------------------------  -------------------  ------------------
Zarr                  184.51                1.8236               0.0050                      25.72                14
HDF5                  140.69                1.3530               0.1167                      145.30               15
redis (PyArrow) [1]_  267.82                1.3794               NA                          68.00                48
redis (pickle) [1]_   305.24                1.7668               NA                          66.75                40 
====================  ====================  ===================  ==========================  ===================  ==================

.. [1] Redis doesn't support larger than memory array access. The total write time is larger than number of arrays times mean write time because redis takes some time to write everything to disk and shut down at the end.

