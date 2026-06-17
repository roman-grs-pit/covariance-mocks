Installation
============

Clone the repository on Perlmutter:

.. code-block:: bash

   git clone https://github.com/roman-grs-pit/covariance-mocks.git
   cd covariance-mocks
   git checkout sfr-only

Load the environment, which provides the conda environment and HPC modules:

.. code-block:: bash

   source scripts/load_env.sh

Install the package:

.. code-block:: bash

   pip install -e .

Dependencies
------------

* ``rgrspit_diffsky`` for galaxy modeling
* JAX (GPU)
* NumPy, SciPy, h5py (with parallel HDF5), astropy
* mpi4py
* PyYAML

Verification
------------

.. code-block:: bash

   source scripts/load_env.sh
   pytest -m "unit or (system and not slow)" -v
   pytest tests/test_selection.py -v
