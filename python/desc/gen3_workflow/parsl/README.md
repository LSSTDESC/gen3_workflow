## Using ctrl_bps and the Parsl-based plug-in to run Rubin Science Pipelines

### Requirements
* `lsst_distrib` `w_2021_03` or later*
* `parsl`
* `gen3_workflow`
* A Gen3 repository

### Software Set Up
* For using the batch queues at NERSC, a CVMFS installation of `lsst_distrib` is recommended.  An `lsstsqre` shifter image cannot be used since the runtime environment on the submission node uses Slurm commands such as `sbatch` to interact with the NERSC batch queues and those commands are not available in those images.
* If one is running locally on a single node, then a shifter image can be used.
* Assuming the v1.0.0 version or later isn't already available, Parsl can be installed with `pip install parsl --user` in one's `~/.local` folder.
* Finally, this `gen3_workflow` package is needed.  To install and set it up do
```
$ git clone https://github.com/LSSTDESC/gen3_workflow.git
$ cd gen3_workflow
$ git checkout u/jchiang/gen3_scripts
$ setup -r . -j
```
Note that the `setup` command must be issued after setting up `lsst_distrib`.

### Editing the bps config file
An example file is available [here](https://github.com/LSSTDESC/gen3_workflow/blob/u/jchiang/gen3_scripts/examples/bps_DRP.yaml).  The key entries that need to be modified are
```
payload:
  butlerConfig: ${PWD}/gen3-repo
  inCollection: LSSTCam-imSim/defaults
  outCollection: shared/parsl_patch_test_{timestamp}
  dataQuery: tract=3828 AND patch=24 AND skymap='DC2'

parslConfig: desc.gen3_workflow.parsl.thread_pool_config_4
#parslConfig: desc.gen3_workflow.parsl.thread_pool_config_32
#parslConfig: desc.gen3_workflow.parsl.ht_debug_config
#parslConfig: desc.gen3_workflow.parsl.knl_htx_config
```
* `butlerConfig` should point to your Gen3 repo.
* `inCollection` is the list of input collections.
* `outCollection` is the name you give to your output collection.  The `{timestamp}` field ensures that unique collection names are assigned.
* `dataQuery` is the data selection to be made.  The above example selects a single patch for processing using the `DC2` skymap.
* `parslConfig` specifies which resources to use for running the pipetask jobs.  The two `thread_pool_confg`s specify a maximum of 4 and 32 concurrent threads to be used.  The `ht_debug_config` is configured to submit to the debug queue running on a Haswell node, and `knl_htx_config` uses Parsl's [`HighThroughputExecutor`](https://parsl.readthedocs.io/en/stable/userguide/execution.html#executors) to submit to KNL batch queues at NERSC.

### Running the Pipeline
Running the pipeline code consists of entering
```
$ bps submit bps_DRP.yaml
```
where `bps_DRP.yaml` is the bps config file.   If you are using `ht_debug_config` or `knl_htx_config`, then a `runinfo` subdirectory will be created which contains Parsl log ouput.  The ctrl_bps code writes to a `submit` subdirectory which contains the QuantumGraph files that are used to run each quantum of processing and output logs that appear in a folder `submit/<outCollection>/logging`.

Note that parsl requires a running python instance, so the `bps submit` command will continue running as long as the underlying pipeline is executing.

### Running a Pipeline from the Python prompt
Since Parsl requires an active python instance, running pipelines usually works better doing so from a python prompt.  There's a helper function to enable that:
```
>>> from desc.gen3_workflow.parsl import start_pipeline

>>> graph = start_pipeline('bps_DRP.yaml')
INFO  2021-02-10T05:13:25.047Z ctrl.mpexec.cmdLineFwk ()(cmdLineFwk.py:528)- QuantumGraph contains 29 quanta for 13 tasks, graph ID: '1612933997.1458979-461'

real	0m6.199s
user	0m5.493s
sys	0m0.532s
```
The `start_pipeline` function will initialize the pipeline and compute the QuantumGraph for the `pipelineYaml`, `butlerConfig`, `inCollection` and `dataQuery` that are specified in the bps config file, but it will not launch any jobs.   The `start_pipeline` function will return a `ParslGraph` object that allows one to control the execution of the pipeline and also provides ways to inspect the status of a running pipeline.

One can obtain the overall status with
```
>>> graph.status()
task type                   pending  running  succeeded  failed  total

isr                               5        0          0       0      5
characterizeImage                 5        0          0       0      5
calibrate                         5        0          0       0      5
makeWarp                          1        0          0       0      1
consolidateVisitSummary           1        0          0       0      1
skyCorrectionTask                 5        0          0       0      5
measure                           1        0          0       0      1
assembleCoadd                     1        0          0       0      1
detection                         1        0          0       0      1
forcedPhotCoadd                   1        0          0       0      1
deblend                           1        0          0       0      1
mergeDetections                   1        0          0       0      1
mergeMeasurements                 1        0          0       0      1
```
Here we see the different types of tasks (as given by the task labels in the pipeline yaml file), and the execution state of each task or job.

The `ParslGraph` object has a reference to the `BpsConfig` object that contains the values set in the bps config file.  For this example, the `dataQuery` value shows that we are processing data from a single visit that overlaps with a particular patch:
```
>>> print(graph.config['dataQuery'])
tract=3828 AND patch=24 AND skymap='DC2' AND visit=192355
```
To run the full pipeline, just call the `run` command:
```
>>> graph.run()
```
This function traverses the full pipeline DAG, assigns each job to specific resources, passes the dependences to Parsl, and Parsl manages the job executions using the specified resources.

While the jobs are running, one can print the pipeline status at any time:
```
>>> graph.status()
task type                   pending  running  succeeded  failed  total

isr                               0        1          4       0      5
characterizeImage                 2        3          0       0      5
calibrate                         5        0          0       0      5
consolidateVisitSummary           1        0          0       0      1
makeWarp                          1        0          0       0      1
skyCorrectionTask                 5        0          0       0      5
measure                           1        0          0       0      1
assembleCoadd                     1        0          0       0      1
forcedPhotCoadd                   1        0          0       0      1
detection                         1        0          0       0      1
mergeDetections                   1        0          0       0      1
deblend                           1        0          0       0      1
mergeMeasurements                 1        0          0       0      1
```

#### Saving state and restarting pipelines
Each time `start_pipeline` is called, it generates a new pipeline instance, and it uses the `outCollection` parameter to set the location of the QuantumGraph files it writes for each job, and it uses that collection name in the registry of the data repository.  Database conflicts will occur if a pipeline instance tries to reuse an existing collection name, so the `outCollection` values can use the `timestamp` variable to ensure that the output collection names are unique, e.g., in our example bps yaml config file, we have
```
  outCollection: shared/parsl_3828_24_192355/{timestamp}
```
and this will produce a unique collection name:
```
>>> print(graph.config['outCollection'])
shared/parsl_3828_24_192355/20210210T051311Z
```

However, if the python session terminates before the pipeline jobs finish, we need a way of recovering that pipeline instance without re-initializing the full pipeline.  We can do this by saving the config attribute of the `ParslGraph` object as a pickle file:
```
>>> graph.save_config('drp_3828_24_192355.pickle')
```
This can be done at any time while the pipeline is running, but it's useful to do it as soon as the `start_pipeline` function returns the `ParslGraph` object.

The `ParslGraph` associated with that pickled config can restored in a new python session:
```
>>> from desc.gen3_workflow.parsl import ParslGraph
>>> graph = ParslGraph.restore('drp_3828_24_192355.pickle')
```
Since the pipeline state is saved on disk in the registry database, in the associate `ctrl_bps` files in the `submit` folder, and in the log file output, the status of the restored graph will reflect the state of the pipeline as it was encoded in those locations when the python session ended:
```
>>> graph.status()
task type                   pending  running  succeeded  failed  total

isr                               0        0          5       0      5
characterizeImage                 1        0          3       1      5
calibrate                         5        0          0       0      5
consolidateVisitSummary           1        0          0       0      1
makeWarp                          1        0          0       0      1
skyCorrectionTask                 5        0          0       0      5
measure                           1        0          0       0      1
assembleCoadd                     1        0          0       0      1
forcedPhotCoadd                   1        0          0       0      1
detection                         1        0          0       0      1
mergeDetections                   1        0          0       0      1
deblend                           1        0          0       0      1
mergeMeasurements                 1        0          0       0      1
```
The pipeline can be restarted from this point simply by issuing the `run` command again:
```
>>> graph.run()
>>> graph.status()
task type                   pending  running  succeeded  failed  total

isr                               0        0          5       0      5
characterizeImage                 0        1          3       1      5
calibrate                         3        2          0       0      5
consolidateVisitSummary           1        0          0       0      1
makeWarp                          1        0          0       0      1
skyCorrectionTask                 5        0          0       0      5
measure                           1        0          0       0      1
assembleCoadd                     1        0          0       0      1
forcedPhotCoadd                   1        0          0       0      1
detection                         1        0          0       0      1
mergeDetections                   1        0          0       0      1
deblend                           1        0          0       0      1
mergeMeasurements                 1        0          0       0      1
```
Since some jobs may have terminated before finishing when the python instance stopped, they'll appear in the `running` state even though they aren't actually running, but eventually Parsl *will* execute any job that hasn't either finished successfully or failed with an error.
