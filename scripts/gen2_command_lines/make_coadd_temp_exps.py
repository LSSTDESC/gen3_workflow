import os
import sys
from desc.gen3_workflow import CommandLineGenerator, dispatch_commands, \
    Gen2Products, missing_outputs

sensor_visit_skymap_file = 'tracts_mapping_y01.pkl'
parent_repo = ('/global/cfs/cdirs/lsst/production/DC2_ImSim/Run2.2i'
               '/desc_dm_drp/v19.0.0-v1/rerun/run2.2i-calexp-v1')
local_repo = '/global/cscratch1/sd/jchiang8/desc/Run2.2i/DR2/repo_multiproc'
bands = 'ugrizy'
log_dir = 'logging'

cl_gen = CommandLineGenerator(sensor_visit_skymap_file, parent_repo,
                              local_repo, bands=bands, log_dir=log_dir)

tract = 3828
patches = sorted(list(set(cl_gen.df0.query(f'tract=={tract}')['patch'])))

rerun = 'coadd'
command_dict = dict()
for patch in patches:
    print(patch)
    sys.stdout.flush()
    command_dict.update(cl_gen.get_per_visit_cls('makeCoaddTempExp',
                                                 rerun, tract, patch))

commands = []
gen2_products = Gen2Products(os.path.join(local_repo, 'rerun', rerun))
for key, command in command_dict.items():
    if missing_outputs(gen2_products.makeCoaddTempExp_outputs(*key)):
        commands.append(command)

processes = 32
dry_run = True
print(f'# commands: {len(commands)}')
sys.stdout.flush()
dispatch_commands(commands, processes=processes, dry_run=dry_run)
