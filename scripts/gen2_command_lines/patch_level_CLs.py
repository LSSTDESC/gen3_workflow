import os
import subprocess
import multiprocessing
import pandas as pd
import lsst.daf.persistence as dp
from desc.gen3_workflow import CommandLineGenerator, dispatch_commands

sensor_visit_skymap_file = 'tracts_mapping_y01.pkl'
parent_repo = ('/global/cfs/cdirs/lsst/production/DC2_ImSim/Run2.2i'
               '/desc_dm_drp/v19.0.0-v1/rerun/run2.2i-calexp-v1')
local_repo = '/global/cscratch1/sd/jchiang8/desc/Run2.2i/DR2/repo_multiproc'
log_dir = 'logging'

bands = 'ugrizy'

cl_gen = CommandLineGenerator(sensor_visit_skymap_file, parent_repo,
                              local_repo, bands=bands, log_dir=log_dir)

tract = 3828
patches = sorted(list(set(cl_gen.df0.query(f'tract=={tract}')['patch'])))

rerun = 'coadd'
make_coadd_temp_exp = cl_gen.get_per_visit_cls('makeCoaddTempExp',
                                               rerun, tract, patch)
assemble_coadd = cl_gen.get_visit_list_cls('assembleCoadd',
                                           rerun, tract, patch)
rerun = 'coadd:coaddPhot'
detect_coadd_sources = cl_gen.get_ftp_cls('detectCoaddSources',
                                                   rerun, tract, patch)
rerun = 'coaddPhot'
merge_coadd_detections = cl_gen.get_merge_task_cl('mergeCoaddDetections',
                                                  rerun, tract, patch)
deblend_coadd_sources = cl_gen.get_ftp_cls('deblendCoaddSources',
                                           rerun, tract, patch)
measure_coadd_sources = cl_gen.get_ftp_cls('measureCoaddSources',
                                           rerun, tract, patch)
merge_coadd_measurements = cl_gen.get_merge_task_cl('mergeCoaddMeasurements',
                                                    rerun, tract, patch)
forced_phot_coadd = cl_gen.get_ftp_cls('forcedPhotCoadd',
                                       rerun, tract, patch)

dry_run = True

processes = 25
dispatch_commands(make_coadd_temp_exp, processes=processes, dry_run=dry_run)

processes = 6
dispatch_commands(assemble_coadd, processes=processes, dry_run=dry_run)

dispatch_commands(detect_coadd_sources, processes=processes, dry_run=dry_run)

if dry_run:
    print(merge_coadd_detections)
    print()
else:
    subprocess.check_call(merge_coadd_detections, shell=True)

dispatch_commands(deblend_coadd_sources, processes=processes, dry_run=dry_run)

dispatch_commands(measure_coadd_sources, processes=processes, dry_run=dry_run)

if dry_run:
    print(merge_coadd_measurements)
    print()
else:
    subprocess.check_call(merge_coadd_measurements, shell=True)

dispatch_commands(forced_phot_coadd, processes=processes, dry_run=dry_run)
