import os
import sys
import subprocess
import multiprocessing
from desc.gen3_workflow import CommandLineGenerator, dispatch_commands

sensor_visit_skymap_file = 'tracts_mapping_y01.pkl'
parent_repo = ('/global/cfs/cdirs/lsst/production/DC2_ImSim/Run2.2i'
               '/desc_dm_drp/v19.0.0-v1/rerun/run2.2i-calexp-v1')
local_repo = '/global/cscratch1/sd/jchiang8/desc/Run2.2i/DR2/repo_multiproc'
bands = 'ugrizy'
log_dir = 'logging'

cl_gen = CommandLineGenerator(sensor_visit_skymap_file, parent_repo,
                              local_repo, bands=bands, log_dir=log_dir)

rerun = 'coaddPhot'
tract = 3828
patches = sorted(list(set(cl_gen.df0.query(f'tract=={tract}')['patch'])))

commands = []
for patch in patches:
    commands.append(
        cl_gen.get_merge_task_cl('mergeCoaddMeasurements', rerun, tract, patch))

print(len(commands))
processes = 32
dry_run = True
dispatch_commands(commands, processes=processes, dry_run=dry_run)
