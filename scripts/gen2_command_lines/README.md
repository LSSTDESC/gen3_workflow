This folder contains example scripts that run the coadd/multiband pipe
tasks for tract 3828 using the existing visit-level data at NERSC.

To use the code in this repo, it needs to be set up with eups:

```
$ cd <to your working directory>
$ git clone https://github.com/LSSTDESC/gen3_workflow.git
$ cd gen3_workflow
$ git checkout u/jchiang/gen2_scripts      # if still not merged
$ setup -r . -j
```

This assumes that the lsst_distrib v19.0.0 environment has been set
up.  Updated configs for obs_lsst will also be needed:

```
$ cd ..
$ git clone https://github.com/lsst/obs_lsst.git
$ cd obs_lsst
$ git checkout u/jchiang/pipe_task_config_updates
$ setup -r . -j
$ cd ..
```

Assuming that one has a parent repo with the visit-level data
(`processCcd.py` outputs and `skyCorrection.py` outputs) in
`parent_repo` (as specified in example scripts) one would run each
script in sequence:

```
$ python reformat_tracts_mapping.py

$ python make_coadd_temp_exps.py

$ python assemble_and_detect_coadds.py

$ python merge_coadd_detections.py

$ python deblend_and_measure_coadds.py

$ python merge_coadd_measurements.py

$ python forced_phot_coadd.py
```

Each script has a `dry_run` variable that if set to `False` will just
print out the pipe task command lines that will be executed.  In
addition to the paths to the parent and local repos , the `processes`
variable can be set to do the appropriate level of parallelization for
the available compute resources.

If a particular script times out, it can be rerun and the command line
instances that already have their data products in the output repo
will be skipped.
