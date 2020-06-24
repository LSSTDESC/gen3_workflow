import os
import sys
import subprocess
import multiprocessing
from desc.gen3_workflow import CommandLineGenerator, dispatch_commands,\
    Gen2Products, missing_outputs

sensor_visit_skymap_file = 'tracts_mapping_y01.pkl'
parent_repo = ('/global/cfs/cdirs/lsst/production/DC2_ImSim/Run2.2i'
               '/desc_dm_drp/v19.0.0-v1/rerun/run2.2i-calexp-v1')
local_repo = '/global/cscratch1/sd/jchiang8/desc/Run2.2i/DR2/repo_multiproc'
bands = 'ugrizy'
log_dir = 'logging'

cl_gen = CommandLineGenerator(sensor_visit_skymap_file, parent_repo,
                              local_repo, bands=bands, log_dir=log_dir)

rerun = 'coaddPhot'
tract = 4849
#patches = sorted(list(set(cl_gen.df0.query(f'tract=={tract}')['patch'])))
patches = '0,0 0,1 0,2 0,3 0,4 0,5 0,6 1,0 1,1 1,2 1,3 1,4 1,5'.split()

gen2_products = Gen2Products(os.path.join(local_repo, 'rerun', 'coaddPhot'))

commands = []
for patch in patches:
    if missing_outputs(
            gen2_products.merge_task_outputs('mergeCoaddMeasurements',
                                             tract, patch)):
        commands.append(
            cl_gen.get_merge_task_cl('mergeCoaddMeasurements', rerun,
                                     tract, patch))

print(len(commands))
processes = 5
dry_run = True
dispatch_commands(commands, processes=processes, dry_run=dry_run)
