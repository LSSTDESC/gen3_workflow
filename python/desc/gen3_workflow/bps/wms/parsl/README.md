## Using ctrl_bps and the Parsl-based plug-in to run Rubin Science Pipelines

### Requirements
* `lsst_distrib` `w_2020_47` or later*
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
  inCollection: LSSTCam-imSim/raw/all,LSSTCam-imSim/calib,ref_cat,skymaps/imsim
  outCollection: shared/parsl_patch_test_{timestamp}
  dataQuery: tract=3828 AND patch=24 AND skymap='DC2'

parslConfig: desc.gen3_workflow.bps.wms.parsl.threaded_pool_config_4
#parslConfig: desc.gen3_workflow.bps.wms.parsl.threaded_pool_config_32
#parslConfig: desc.gen3_workflow.bps.wms.parsl.ht_debug_config
```
* `butlerConfig` should point to your Gen3 repo.
* `inCollection` is the list of input collections.
* `outCollection` is the name you give to your output collection.  The `{timestamp}` field ensures that unique collection names are assigned.
* `dataQuery` is the data selection to be made.  The above example selects a single patch for processing using the `DC2` skymap.
* `parslConfig` specifies which resources to use for running the pipetask jobs.  The two `threaded_pool_confg`s specify a maximum of 4 and 32 concurrent threads to be used; and the `ht_debug_config` is configured to submit to the debug queue running on a Haswell node.

### Running the Pipeline
Running the pipeline code consists of entering
```
$ bps submit bps_DRP.yaml
```
where `bps_DRP.yaml` is the bps config file.   If you are using `ht_debug_config` (or another parsl config that uses a [`HighThroughputExecutor`](https://parsl.readthedocs.io/en/stable/userguide/execution.html#executors)), then a `runinfo` subdirectory will be created which contains Parsl log ouput.  The ctrl_bps code writes to a `submit` subdirectory which contains the QuantumGraph files that are used to run each quantum of processing and output logs that appear in a folder `submit/<outCollection>/logging`.

Note that parsl requires a running python instance, so the `bps submit` command will continue running as long as the underlying pipeline is executing.
