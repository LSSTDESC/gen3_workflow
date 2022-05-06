Required packages for running bps with the Parsl plugin at NERSC
================================================================

Requirements
------------
* ``lsst_distrib w_2022_08`` or later
* ``ndcctools``
* ``parsl``
* ``gen3_workflow``
* A Gen3 repository

Software Installation and Set Up using ``lsst_distrib`` on CVMFS
----------------------------------------------------------------
For using the batch queues at NERSC, a CVMFS installation of
``lsst_distrib`` is recommended, although a shifter image could also
be used if running on just a single node (see below).  An ``lsstsqre``
shifter image cannot be used since the runtime environment on the
submission node uses Slurm commands such as **srun** to
interact with the NERSC batch queues and those commands are not
available in those images.  However, if one is running locally on a
single node, then a shifter image can be used.

To set-up the CVMFS installation of ``lsst_distrib`` do

.. code-block:: BASH

  $ source /cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/w_2022_08/loadLSST-ext.bash
  $ setup lsst_distrib
  $ export OMP_NUM_THREADS=1
  $ export NUMEXPR_MAX_THREADS=1

Setting ``OMP_NUM_THREADS=1`` and ``NUMEXPR_MAX_THREADS=1`` prevents
code using libraries such as linpack or blas from using all available
threads on a node.

Since the LSST code currently uses python3.8, one should install a
compatible version of ``ndcctools``. One can find the latest version
of ``ndcctools`` compatible with the python version for the LSST stack
by runnng **conda search -c conda-forge ndcctools**.  Since
``ndcctools`` package is installed with conda, and the CVMFS
distributions are write-protected, it's useful to set up a local area
to do that installation.  The following assumes that ``lsst_distrib``
has been set up:

.. code-block:: BASH

  $ wq_env=`pwd -P`/wq_env
  $ conda create --prefix ${wq_env}
  $ conda activate --stack ${wq_env}
  $ conda install -c conda-forge ndcctools=7.3.0=py38h4630a5e_0 --no-deps

The ``--no-deps`` option prevents conda from trying to replace various
packages in the LSST distribution with more recent versions that are
incompatible with the LSST code.

Currently, one should use the ``desc`` branch of parsl, which can be
installed with

.. code-block:: BASH

  $ pip install --prefix ${wq_env} --no-deps 'parsl[monitoring,workqueue] @ git+https://github.com/parsl/parsl@desc'

Because of the use of the ``--no-deps`` option, several additional
packages will then need to be installed separately:

.. code-block:: BASH

  $ pip install --prefix ${wq_env} typeguard tblib paramiko dill globus-sdk sqlalchemy_utils zmq

With ``ndcctools`` and ``parsl`` installed like this, the ``PYTHONPATH`` and
``PATH`` environment variables need to be updated:

.. code-block:: BASH

  $ export PYTHONPATH=${wq_env}/lib/python3.8/site-packages:${PYTHONPATH}
  $ export PATH=${wq_env}/bin:${PATH}

If desired, an existing installation of these packages can be used via

.. code-block:: BASH

  $ wq_env=/global/cfs/cdirs/desc-co/jchiang8/wq_env

Finally, the ``gen3_workflow`` package is needed.  To install and set it up, do

.. code-block:: BASH

  $ git clone https://github.com/LSSTDESC/gen3_workflow.git
  $ cd gen3_workflow
  $ setup -r . -j

Note that this **setup** command must be issued after setting
up ``lsst_distrib``.

Using a shifter image
---------------------
If one is running on a single node, the environment can be set up with
a shifter image instead of using ``lsst_distrib`` from CVMFS.  NERSC
has `instructions on using shifter
<https://docs.nersc.gov/development/shifter/how-to-use/>`__, and the
following Dockerfile can be used to build an image with the required
packages:

.. code-block:: YAML

  from lsstsqre/centos:7-stack-lsst_distrib-w_2022_08

  RUN source /opt/lsst/software/stack/loadLSST.bash &&\
      setup lsst_apps &&\
      pip install pep8 &&\
      pip install pylint &&\
      pip install ipython &&\
      pip install nose &&\
      pip install jupyter &&\
      pip install -U --no-deps 'parsl[monitoring,workqueue] @ git+https://github.com/parsl/parsl@desc' &&\
      pip install typeguard &&\
      pip install tblib &&\
      pip install paramiko &&\
      pip install dill &&\
      pip install globus-sdk &&\
      pip install sqlalchemy_utils &&\
      conda install -c conda-forge ndcctools=7.3.4=py38h4630a5e_0 --no-deps

As usual, the ``lsst-distrib`` weekly version and version of
``ndcctools`` should be chosen as appropriate.
