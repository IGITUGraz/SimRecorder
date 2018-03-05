import h5py
import numpy as np

f = h5py.File('data.h5', 'w')

for i in range(10):
    f.create_dataset("train/val/{}".format(i), data=np.random.rand(10,10))

for i in range(10):
    f.create_dataset("test/val/{}".format(i), data=np.random.rand(10,10))

f.close()
