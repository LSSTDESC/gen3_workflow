Introduction
------------

Basic user-level documentation for bps is available in the `bps
quickstart guide
<https://github.com/lsst/ctrl_bps/blob/main/doc/lsst.ctrl.bps/quickstart.rst>`__
in the `ctrl_bps <https://github.com/lsst/ctrl_bps>`__ repo.  Note,
however, that the Parsl plugin doesn't support all of the features
described in that guide.  This is largely because bps has been
designed to use a workflow/batch system like HTCondor, where each job
is individually scheduled to use whatever resources are available
across the batch system.  For the LSST Science Pipelines workflows,
which can consist of 10s of thousands of jobs (many of which have run
times of minutes), this sort of scheduling is a poor fit for HPC
centers like NERSC.  So instead, the Parsl plugin relies on a "pilot
job" to reserve a number of whole nodes for several hours and uses the
Parsl workflow manager to schedule the pipetask jobs on that set of
nodes.

Available bps Commands
--------------------------

:command:`bps submit <bps yaml>`
  This command executes the pipeline from end-to-end.  At the start, a
  subdirectory in the :file:`submit` folder in the cwd is created where
  bps and Parsl store the following assets, which are needed for
  executing and managing the workflow:

  * The QuantumGraph (QG) that defines the workflow DAG.
  * The main copy of the execution butler repository, if the execution
    butler is used.
  * :file:`tmp_repos` folder to contain the temporary per-pipetask copies
    of the execution butler repo.
  * :file:`logging` folder to contain log files for each pipetask.
  * A copy of the as-run bps yaml file.
  * Log files for the creation of the QG and main execution butler repo.
  * :file:`final_job.bash`, a bash script that performs the final merge
    step of the workflow jobs into the central destination repository.
  * :file:`parsl_graph_config.pickle`, which contains a copy of the
    :py:class:`ParslGraph` object that encapsulates the workflow
    information used by Parsl.

  In addition, Parsl creates a :file:`runinfo` folder in the cwd which
  contains Parsl-specific log files for each run as well as a
  :file:`monitoring.db` file that's used to keep track of the status
  of the workflows.  Once those assets are in place, Parsl runs the
  pipetask jobs on the reserved nodes.

:command:`bps prepare <bps yaml>`
  This command creates all of the assets mentioned above, but it stops
  short of running the pipetask jobs.  Since the QG generation step
  only uses a single core and can take up to several hours for large
  workflows, it's useful to run :command:`bps prepare` separately
  first, outside of a pilot job, then use a pilot job and
  :command:`bps restart` to run the full workflow.

:command:`bps restart --id <workflow_name>`
  If the pilot job times out, is cancelled, or otherwise stops before
  all of the pipetask jobs in a workflow have run, one can restart the
  workflow using the :command:`bps restart` command.  The
  ``workflow_name`` is printed to the screen when either :command:`bps
  submit` or :command:`bps prepare` are run.  The workflow name has
  the form of ``{payloadName}/{timestamp}`` as defined by those
  elements in the bps yaml file (see below).  These names are also
  used as the folder names for each run in the :file:`submit` directory.

Note that :command:`bps restart` must be executed from the same
directory where :command:`bps submit` or :command:`bps prepare` were
run.

Example bps Configuration File
------------------------------

Workflows run under bps are defined using yaml files.  These contain a
lot of the same information as the pipetask command lines, e.g., the
pipeline yaml to use, the data repository, the input collections,
the data query, etc..  They also have plugin-specific information, as well
as options to specify expected resource usage for individual task types.
Here's an example for the Parsl plugin:

.. code-block:: YAML

   includeConfigs:
     - ${GEN3_WORKFLOW_DIR}/python/desc/gen3_workflow/etc/bps_drp_baseline.yaml
     - ${GEN3_WORKFLOW_DIR}/examples/bps_DC2-3828-y1_resources.yaml

   pipelineYaml: "${GEN3_WORKFLOW_DIR}/pipelines/DRP.yaml"

   payload:
     payloadName: drp_4430_24
     butlerConfig: /global/cfs/cdirs/lsst/production/gen3/DC2/Run2.2i/repo
     inCollection: LSSTCam-imSim/defaults
     dataQuery: "skymap='DC2' and tract=4430 and patch=24"

   parsl_config:
     retries: 1
     monitoring: true
     log_level: logging.WARNING
     executor: WorkQueue
     provider: Local
     nodes_per_block: 10
     worker_options: "--memory=90000"
     #executor: ThreadPool
     #max_threads: 4

The first entry under the ``includedConfigs`` section sets configuration
parameters for the Parsl plugin that override default values defined in
the ctrl_bps package.  The second entry under that section points to a
yaml file with per-pipetask resource requirements that were estimated from
running on DC2 one-year depth WFD observations of tract 3828.  These resource
specifications can be overridden in the submission yaml file.

The ``pipelineYaml`` and ``payload`` sections would be the same as for
any other plugin, and are described in the `bps quickstart guide
<https://github.com/lsst/ctrl_bps/blob/main/doc/lsst.ctrl.bps/quickstart.rst>`__.

The ``parsl_config`` section defines the resources available for
processing and how Parsl will manage those resources.  The first three
items pertain to all ``parsl_config`` configurations, and the
remaining ones are specific to the `Parsl executor
<https://parsl.readthedocs.io/en/stable/userguide/execution.html#executors>`__
used:

``retries``
  This is the number of retries per pipetask job.

``monitoring``
  This is a flag to enable or disable Parsl monitoring. This must be
  set to ``True`` if workflow status summaries are desired.

``log_level``
  This is python logging log-level to use for the Parsl log files.
  Because of the way bps controls the logging at the application
  level, Parsl logging is (unfortunately) echoed to stderr.

``executor``
  The Parsl plugin supports two Parsl executors, ``WorkQueue`` and
  ``ThreadPool``.  The ``WorkQueue`` executor allows for multiple
  nodes to be used in a submission and uses the per-pipetask resource
  requests to manage how jobs are scheduled given the available
  resources. The ``ThreadPool`` executor runs on local resources,
  i.e., using just the node where the main bps thread is running.

``provider``
  For running at NERSC under Slurm, ``Local`` should be used.
  Providers for other batch systems can be implemented/enabled. This
  is only relevant for the ``WorkQueue`` executor.

``nodes_per_block``
  This should be set to the number of nodes requested in the slurm
  pilot job sbatch script.  Relevant only for ``WorkQueue``.

``worker_options``
  These are options to pass the the Parsl ``WorkQueue``
  executor. Currently, the only relevant one is the memory available
  per node where the value provided to ``--memory=`` is in MB.  This
  sets the amount of memory that ``WorkQueue`` allocates per node.  In
  practice, it should be set to ~90% of the total memory per node.
  For Cori-KNL, this would be around 90GB, while for Cori-Haswell,
  120GB would work.  Setting this too high will likely lead to node
  failures owing to out-of-memory (OOM) conditions.

``max_threads``
  This sets the maximum number of concurrent processes that Parsl will
  try to run.  The ``ThreadPool`` executor does not use the
  per-pipetask resource requests information, so setting
  ``max_threads`` too high could lead to OOM failures for certain
  pipetasks.  Relevant only for ``ThreadPool``.

Example sbatch and setup scripts
--------------------------------

Here's an example sbatch script for running on Cori-KNL at NERSC:

.. code-block:: bash

   #!/bin/bash
   #SBATCH --job-name=coadds_ddf_y1-y2_4849
   #SBATCH --nodes=10
   #SBATCH --time=10:00:00
   #SBATCH --constraint=knl
   #SBATCH --qos=regular
   #SBATCH --exclusive
   #SBATCH --account=m1727

   cd <working_directory>
   source ./setup.sh            # script to set up the LSST stack etc.
   bps submit <bps config yaml>

When using multiple nodes, the slurm commands need to be available to
the Parsl workflow manager, so shifter images can't be used as the
runtime environment.  Here's an example setup script that uses the
CVMFS distributions of the LSST stack, a local copy of the
gen3_workflow repo, and installations of Parsl and associated packages
on cfs:

.. code-block:: bash

  weekly_version=w_2022_16
  LSST_DISTRIB=/cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/${weekly_version}
  source "${LSST_DISTRIB}/loadLSST-ext.bash"
  setup lsst_distrib
  setup -r ./gen3_workflow -j
  export OMP_NUM_THREADS=1
  export NUMEXPR_MAX_THREADS=1
  wq_env=/global/cfs/cdirs/desc-co/jchiang8/wq_env
  export PYTHONPATH=${wq_env}/lib/python3.8/site-packages:${PYTHONPATH}
  export PATH=${wq_env}/bin:${PATH}

The ``weekly_version`` can be set to different weekly if desired.

Workflow Status Summary
-----------------------

The status of a workflow can be displayed with the
:command:`workflow_summary.py` executable, e.g.,

.. code-block:: bash

  $ workflow_summary.py u/lsst/drp_3828_24/20220425T032138Z

  task_type                  pending   launched    running  exec_done     failed   dep_fail      total
  isr                              0          0          0        104          0          0        104
  characterizeImage                0          0          0        104          0          0        104
  calibrate                        0          0          0        104          0          0        104
  writeSourceTable                 0          0          0        104          0          0        104
  consolidateVisitSummary          0          0          0         24          0          0         24
  transformSourceTable             0          0          0        104          0          0        104
  makeWarp                         0          0          0         24          0          0         24
  consolidateSourceTable           0          0          0         24          0          0         24
  assembleCoadd                    0          0          0          6          0          0          6
  detection                        0          0          0          6          0          0          6
  mergeDetections                  0          0          0          1          0          0          1
  deblend                          0          0          0          1          0          0          1
  measure                          0          0          0          6          0          0          6
  mergeMeasurements                0          0          0          1          0          0          1
  forcedPhotCoadd                  0          0          0          6          0          0          6
  writeObjectTable                 0          0          0          1          0          0          1
  transformObjectTable             0          0          0          1          0          0          1
  consolidateObjectTable           0          0          0          1          0          0          1

This shows the status of a workflow that successfully executed a
subset of the pipetasks for a small test data set comprising just the
CCD-visits covering patch 24 in tract 3828 with 5 visits per band.
