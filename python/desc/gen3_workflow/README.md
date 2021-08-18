## Using ctrl_bps and the Parsl-based plug-in to run LSST Science Pipelines at NERSC

### Requirements
* `lsst_distrib` `w_2021_33` or later
* `ndcctools`
* `parsl`
* `gen3_workflow`
* A Gen3 repository

### Software Set Up
* For using the batch queues at NERSC, a CVMFS installation of `lsst_distrib` is recommended.  An `lsstsqre` shifter image cannot be used since the runtime environment on the submission node uses Slurm commands such as `sbatch` to interact with the NERSC batch queues and those commands are not available in those images.  However, if one is running locally on a single node, then a shifter image can be used.
* To set-up the CVMFS installation of `lsst_distrib` do
```
$ source /cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/w_2021_33/loadLSST.bash
$ setup lsst_distrib
$ export OMP_NUM_THREADS=1
```
Setting `OMP_NUM_THREADS=1` prevents code using the LINPACK libraries from using all available threads on a node.
* Since the LSST code currently uses python3.8, one should install a compatible version of `ndcctools`.  Since that package is installed with conda, and the CVMFS distributions are write-protected, it's useful to set up a local area to do that installation.  The following assumes that `lsst_distrib` has been set up:
```
$ wq_env=`pwd -P`/wq_env
$ conda create --prefix ${wq_env}
$ conda activate --stack ${wq_env}
$ conda install -c conda-forge ndcctools=7.2.14=py38h4630a5e_0 --no-deps
```
The `--no-deps` option prevents conda from trying to replace various packages in the LSST distribution with more recent versions that are incompatible with the LSST code.
* Currently, one should use the `desc` branch of parsl, which can be installed with
```
pip install --prefix ${wq_env} --no-deps 'parsl[monitoring,workqueue] @ git+https://github.com/parsl/parsl@desc'
```
Because of the `--no-deps` option, several additional packages will then need to be installed separately:
```
pip install --prefix ${wq_env} typeguard tblib paramiko dill globus-sdk sqlalchemy_utils
* With `ndcctools` and `parsl` installed like this, the `PYTHONPATH` and `PATH` environment variables need to be updated:
```
$ export PYTHONPATH=${wq_env}/lib/python3.8/site-packages:${PYTHONPATH}
$ export PATH=${wq_env}/bin:${PATH}
```
If desired, existing installations can be used via
``
$ wq_env=/global/cscratch1/sd/jchiang8/desc/gen3_tests/wq_env_2021-08-10
```
* Finally, this `gen3_workflow` package is needed.  To install and set it up do
```
$ git clone https://github.com/LSSTDESC/gen3_workflow.git
$ cd gen3_workflow
$ git checkout u/jchiang/gen3_scripts
$ setup -r . -j
```
Note that this `setup` command must be issued after setting up `lsst_distrib`.

### Setting up a user area in the gen3-3828-y1 repo
From a local working directory, make a symlink from a local subfolder to the user area in the gen3-3828-y1 repo.  Typically, one's user name is used for this folder name.  The pipeline outputs of your workflow runs will be written under that subfolder.
```
$ cd <working_dir>
$ mkdir <user_name>
$ ln -s ${PWD}/<user_name> /global/cscratch1/sd/jchiang8/desc/gen3_tests/w_2021_12/gen3-3828-y1/u/
```
For convenience, make a local symlink to the gen3-3829-y1 repo:
```
$ ln -s /global/cscratch1/sd/jchiang8/desc/gen3_tests/w_2021_12/gen3-3828-y1 .
```
One can check that the repo is accessible via the butler with:
```
$ butler query-collections gen3-3828-y1 --chains tree

```
This will print all of the collections in that repo.

### Editing the bps config file
A couple of example files is available:  [bps_DRP.yaml](https://github.com/LSSTDESC/gen3_workflow/blob/u/jchiang/gen3_scripts/examples/bps_DRP.yaml) will run the full DRP pipeline, and [bps_SFP.yaml](https://github.com/LSSTDESC/gen3_workflow/blob/u/jchiang/gen3_scripts/examples/bps_SFP.yaml) is set up just to do the visit-level processing that's equivalent to running `processCcd.py` in Gen2.  In order to ensure that the outputs are written to the location given above, one should set the `operator` field:
```
operator: <user_name>
```
and it's also useful to edit the `parsl_config` section to specify the resources you wish to use:
```
parsl_config:
  retries: 1
  monitoring: true
  checkpoint: false
  executor: WorkQueue
  provider: Local
  nodes_per_block: 10
  worker_options: "--memory=90000"
```
This will use the `WorkQueueExecutor` with a `LocalProvider` for 10 nodes with 90,000 MB of memory available per node and with monitoring enabled and checkpoiting disabled.  Another possible configuration is
```
parsl_config:
  retries: 1
  monitoring: false
  executor: ThreadPool
  max_threads: 4
```

### Running the Pipeline
Running the pipeline code consists of entering
```
$ bps -no-log-tty submit bps_DRP.yaml
```
where `bps_DRP.yaml` is the bps config file.  The ctrl_bps code writes to a `submit` subdirectory which contains the QuantumGraph files that are used to run each quantum of processing and output logs that appear in a folder `submit/u/{operator}/{paylodName}/{timestamp}/logging`.

Note that parsl requires a running python instance, so the `bps submit` command will continue running as long as the underlying pipeline is executing.

### Running a Pipeline from the Python prompt
Since Parsl requires an active python instance, running pipelines usually works better doing so from a python prompt.  There's a helper function to enable that:
```
>>> from desc.gen3_workflow import start_pipeline

>>> graph = start_pipeline('bps_DRP.yaml')
INFO  2021-02-10T05:13:25.047Z ctrl.mpexec.cmdLineFwk ()(cmdLineFwk.py:528)- QuantumGraph contains 29 quanta for 13 tasks, graph ID: '1612933997.1458979-461'

real	0m6.199s
user	0m5.493s
sys	0m0.532s
```
The `start_pipeline` function will initialize the pipeline and compute the QuantumGraph for components given in the `payload` section in the bps config file, but it will not launch any jobs.   The `start_pipeline` function will return a `ParslGraph` object that allows one to control the execution of the pipeline and also provides ways to inspect the status of a running pipeline.

One can obtain the overall status with
```
>>> graph.status()
task_type                    pending   launched    running  exec_done     failed   dep_fail      total
isr                              104          0          0          0          0          0        104
characterizeImage                104          0          0          0          0          0        104
calibrate                        104          0          0          0          0          0        104
consolidateVisitSummary           24          0          0          0          0          0         24
makeWarp                          24          0          0          0          0          0         24
selectGoodSeeingVisits             6          0          0          0          0          0          6
assembleCoadd                      6          0          0          0          0          0          6
templateGen                        6          0          0          0          0          0          6
detection                          6          0          0          0          0          0          6
imageDifference                  104          0          0          0          0          0        104
mergeDetections                    1          0          0          0          0          0          1
deblend                            1          0          0          0          0          0          1
measure                            6          0          0          0          0          0          6
mergeMeasurements                  1          0          0          0          0          0          1
forcedPhotCoadd                    6          0          0          0          0          0          6
forcedPhotCcd                    104          0          0          0          0          0        104
forcedPhotDiffim                 104          0          0          0          0          0        104
consolidateDiaSourceTable         24          0          0          0          0          0         24
transformSourceTable             104          0          0          0          0          0        104
transformDiaSourceCat            104          0          0          0          0          0        104
consolidateSourceTable            24          0          0          0          0          0         24
writeSourceTable                 104          0          0          0          0          0        104
healSparsePropertyMaps             6          0          0          0          0          0          6
drpAssociation                     1          0          0          0          0          0          1
consolidateObjectTable             1          0          0          0          0          0          1
drpDiaCalculation                  1          0          0          0          0          0          1
makeVisitTable                     1          0          0          0          0          0          1
writeObjectTable                   1          0          0          0          0          0          1
makeCcdVisitTable                  1          0          0          0          0          0          1
transformObjectTable               1          0          0          0          0          0          1
```
Here we see the different types of tasks (as given by the task labels in the pipeline yaml file), and the execution state of each task or job.

The `ParslGraph` object has a reference to the `BpsConfig` object that contains the values set in the bps config file.  For this example, the `dataQuery` value shows that we are processing data from a single visit that overlaps with a particular patch:
```
>>> print(graph.config['dataQuery'])
skymap='DC2' and tract=3828 and patch=24
```
To run the full pipeline, just call the `run` command:
```
>>> graph.run()
```
This function traverses the full pipeline DAG, sets resource requests for each job, passes the dependences to Parsl, and Parsl manages the job executions using the requested resources.

While the jobs are running, one can print the pipeline status at any time:
```
>>> graph.status()
```

#### Saving state and restarting pipelines
Each time `start_pipeline` is called, it generates a new pipeline instance, and it uses the `outCollection` field to set the location of the QuantumGraph files it writes for each job, and it uses that collection name in the registry of the data repository.  Database conflicts will occur if a pipeline instance tries to reuse an existing collection name, so the `outCollection` values typically use the `timestamp` variable to ensure that the output collection names are unique, e.g., in our example, the `outCollection` field is constructed by default as
```
  outCollection: u/{operator}/{payloadName}/{timestamp}
```
and this will produce a unique collection name:
```
>>> print(graph.config['outCollection'])
u/jchiang/drp_DC2/20210818T194038Z
```
