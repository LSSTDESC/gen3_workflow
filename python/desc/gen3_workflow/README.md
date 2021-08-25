## Using ctrl_bps and the Parsl-based plug-in to run LSST Science Pipelines at NERSC

### Requirements
* `lsst_distrib` `w_2021_33` or later
* `ndcctools`
* `parsl`
* `gen3_workflow`
* A Gen3 repository

### Software Installation and Set Up
For using the batch queues at NERSC, a CVMFS installation of `lsst_distrib` is recommended.  An `lsstsqre` shifter image cannot be used since the runtime environment on the submission node uses Slurm commands such as `sbatch` to interact with the NERSC batch queues and those commands are not available in those images.  However, if one is running locally on a single node, then a shifter image can be used.

To set-up the CVMFS installation of `lsst_distrib` do
```
$ source /cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/w_2021_33/loadLSST.bash
$ setup lsst_distrib
$ export OMP_NUM_THREADS=1
```
Setting `OMP_NUM_THREADS=1` prevents code using the LINPACK libraries from using all available threads on a node.

Since the LSST code currently uses python3.8, one should install a compatible version of `ndcctools`.  Since that package is installed with conda, and the CVMFS distributions are write-protected, it's useful to set up a local area to do that installation.  The following assumes that `lsst_distrib` has been set up:
```
$ wq_env=`pwd -P`/wq_env
$ conda create --prefix ${wq_env}
$ conda activate --stack ${wq_env}
$ conda install -c conda-forge ndcctools=7.3.0=py38h4630a5e_0 --no-deps
```
The `--no-deps` option prevents conda from trying to replace various packages in the LSST distribution with more recent versions that are incompatible with the LSST code.

Currently, one should use the `desc` branch of parsl, which can be installed with
```
$ pip install --prefix ${wq_env} --no-deps 'parsl[monitoring,workqueue] @ git+https://github.com/parsl/parsl@desc'
```
Because of the `--no-deps` option, several additional packages will then need to be installed separately:
```
$ pip install --prefix ${wq_env} typeguard tblib paramiko dill globus-sdk sqlalchemy_utils
```
With `ndcctools` and `parsl` installed like this, the `PYTHONPATH` and `PATH` environment variables need to be updated:
```
$ export PYTHONPATH=${wq_env}/lib/python3.8/site-packages:${PYTHONPATH}
$ export PATH=${wq_env}/bin:${PATH}
```
If desired, existing installations can be used via
```
$ wq_env=/global/cscratch1/sd/jchiang8/desc/gen3_tests/wq_env_2021-08-24
```
Finally, the `gen3_workflow` package is needed.  To install and set it up, do
```
$ git clone https://github.com/LSSTDESC/gen3_workflow.git
$ cd gen3_workflow
$ git checkout u/jchiang/gen3_scripts
$ setup -r . -j
```
Note that this `setup` command must be issued after setting up `lsst_distrib`.

### Setting up a user area in the gen3-3828-y1 repo
From a local working directory, make a symlink from a local subfolder to the user area in the `gen3-3828-y1` repo.  Typically, one's user name is used for this folder name.  The pipeline outputs of your workflow runs will be written under that subfolder.
```
$ cd <working_dir>
$ mkdir <user_name>
$ ln -s ${PWD}/<user_name> /global/cscratch1/sd/jchiang8/desc/gen3_tests/w_2021_12/gen3-3828-y1/u/
```
It's convenient to make a local symlink to the `gen3-3829-y1` repo:
```
$ ln -s /global/cscratch1/sd/jchiang8/desc/gen3_tests/w_2021_12/gen3-3828-y1 .
```
Since the registry in this repo is backed by a postgres database, you'll need a file with db access credentials in your home directory at NERSC.  Contact [jchiang87](https://lsstc.slack.com/team/U2LRMHKJ5) in LSSTC Slack to obtain these.  Once everything is set up, you can check that the repo is accessible via the butler with:
```
$ butler query-collections gen3-3828-y1 --chains tree
                      Name                            Type   
-------------------------------------------------- -----------
LSSTCam-imSim/raw/all                              RUN        
LSSTCam-imSim/calib                                CALIBRATION
LSSTCam-imSim/calib/unbounded                      RUN        
LSSTCam-imSim/calib/20220806T000000Z               RUN        
LSSTCam-imSim/calib/20220101T000000Z               RUN        
LSSTCam-imSim/calib/20231201T000000Z               RUN        
skymaps                                            RUN        
ref_cat                                            RUN        
LSSTCam-imSim/defaults                             CHAINED    
  LSSTCam-imSim/raw/all                            RUN        
  LSSTCam-imSim/calib                              CALIBRATION
  skymaps                                          RUN        
  ref_cat                                          RUN        
shared/sfp_3828_y1/20210223T191036Z                RUN        
shared/coadd_processing_3828_y1/20210319T055155Z   RUN        
shared/drp_pg_test/20210408T150000Z                RUN        
shared/drp_full_test/20210419T202408Z              RUN        
shared/bfk/20210415T142710Z                        RUN        
shared/drp_full_test/20210419T204856Z              RUN        
shared/dia_test/20210420T181323Z                   RUN        
shared/drp_3828/20210521T161048Z                   RUN        
shared/drp_3828/20210521T201925Z                   RUN        
u/jchiang/sfp_3828/20210525T153432Z                RUN        
u/jchiang/coadd_multiband_3828/20210531T215629Z    RUN        
u/jchiang/objectTables_3828/20210608T230612Z       RUN        
shared/bfk/20210618T044003Z                        RUN        
u/jchiang/calibrate_dm-30490_test/20210621T225054Z RUN        
u/jchiang/sfp_3828-y1/20210805T011112Z             RUN        
u/jchiang/sfp_3828-y1/20210805T031935Z             RUN        
u/jchiang/sfp_3828-y1/20210806T223525Z             RUN        
u/jchiang/sfp_3828-y1/20210810T000946Z             RUN        
u/jchiang/sfp_3828-y1/20210811T031616Z             RUN        
```
This prints all of the collections in that repo.

### Editing the bps config file
A couple of example files are available:  [bps_drp.yaml](../../../examples/bps_drp.yaml) is set up to run the full DRP pipeline, and [bps_sfp.yaml](../../../examples/bps_sfp.yaml) is set up for just the visit-level processing that's equivalent to running `processCcd.py` in Gen2.  In order to ensure that the outputs are written to the location given above, one should set the `operator` field at the top of the bps yaml file:
```
operator: <user_name>
```
It's also useful to edit the `parsl_config` section to specify the resources you wish to use:
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
This will use the `WorkQueueExecutor` with a `LocalProvider` for 10 nodes with 90,000 MB of memory available per node; parsl monitoring is enabled and checkpointing is disabled.  Another possible configuration is
```
parsl_config:
  retries: 1
  monitoring: false
  executor: ThreadPool
  max_threads: 4
```

### Running the Pipeline
The pipeline can be run from the command line with
```
$ bps --no-log-tty submit bps_sfp.yaml
```
where `bps_sfp.yaml` is the bps config file. The `--no-log-tty` option suppresses the echoing of the parsl log messages to the terminal by the LSST code.  The `ctrl_bps` code writes to a `submit/u/{operator}/{payloadName}/{timestamp}` subdirectory where the QuantumGraph file, the execution butler files, and other items are written, including log files for each job.

Note that parsl requires a running python instance, so the `bps submit` command will continue running as long as the underlying pipeline is executing.

### Running a Pipeline from the Python prompt
Since Parsl requires an active python instance, running pipelines usually works better doing so from a python prompt.  There's a helper function to enable that:
```
>>> from desc.gen3_workflow import start_pipeline
>>> graph = start_pipeline('bps_sfp.yaml')
INFO 2021-08-18T14:13:13.613-07:00 ctrl.mpexec.cmdLineFwk ()(cmdLineFwk.py:578) - QuantumGraph contains 14028 quanta for 3 tasks, graph ID: '1629320853.0446935-17265'

real	0m28.674s
user	0m25.054s
sys	0m1.557s
```
The `start_pipeline` function will initialize the pipeline and compute the QuantumGraph for components given in the `payload` section of the bps config file, but it will not launch any jobs.   The `start_pipeline` function will return a `ParslGraph` object that allows one to control the execution of the pipeline and also provides ways to inspect the status of a running pipeline.  Depending on the complexity of the pipeline and on the number of inputs, the QuantumGraph and associated file generation can take many minutes to finish.

One can obtain the overall status with
```
>>> graph.status()
task_type                    pending   launched    running  exec_done     failed   dep_fail      total
isr                             4676          0          0          0          0          0       4676
characterizeImage               4676          0          0          0          0          0       4676
calibrate                       4676          0          0          0          0          0       4676
```
Here we see the different types of tasks (as given by the task labels in the pipeline yaml file), and the execution state of each task or job.

The `ParslGraph` object has a reference to the `BpsConfig` object that contains the values set in the bps config file.  For this example, the `dataQuery` value shows that we are processing data for a single tract in the DC2 skymap:
```
>>> print(graph.config['dataQuery'])
skymap='DC2' and tract=3828
```
Before running any jobs, it may be desireable to "pre-stage" the execution butler files associated with each job.  This is done with
```
>>> graph.copy_exec_butler_files()
```
If you skip this step, then the execution butler files will be copied by parsl jobs that are prerequisites for each DRP pipetask job.  Running this way can have the side-effect of throttling the submission of the DRP pipetask jobs.
To run the full pipeline, just call the `run` command:
```
>>> graph.run()
```
This function traverses the full pipeline DAG, sets resource requests for each job, passes the dependencies to parsl, and parsl manages the job executions using the requested resources.

While the jobs are running, one can print the pipeline status at any time:
```
>>> graph.status()
```
