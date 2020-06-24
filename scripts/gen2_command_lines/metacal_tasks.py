import os
import sys
import subprocess
import multiprocessing
from desc.gen3_workflow import CommandLineGenerator, dispatch_commands,\
    Gen2Products

sensor_visit_skymap_file = 'tracts_mapping_y01.pkl'
parent_repo = ('/global/cfs/cdirs/lsst/production/DC2_ImSim/Run2.2i'
               '/desc_dm_drp/v19.0.0-v1/rerun/run2.2i-calexp-v1')
local_repo = '/global/cscratch1/sd/jchiang8/desc/Run2.2i/DR2/repo_multiproc'
bands = 'ugrizy'
log_dir = 'logging'

cl_gen = CommandLineGenerator(sensor_visit_skymap_file, parent_repo,
                              local_repo, bands=bands, log_dir=log_dir)

tract = 4849
#patches = sorted(list(set(cl_gen.df0.query(f'tract=={tract}')['patch'])))
patches = '0,0 0,1 0,2 0,3 0,4 0,5 0,6 1,0 1,1 1,2 1,3 1,4 1,5'.split()

gen2_products = Gen2Products(os.path.join(local_repo, 'rerun', 'metacal'))

command_pairs = dict()
for patch in patches:
    print(patch)
    sys.stdout.flush()
    command_pairs.update(cl_gen.get_metacal_cls('coaddPhot:metacal',
                                                tract, patch))

def run_cls(*commands, dry_run=True):
    for command in commands:
        if dry_run:
            print(command)
            print()
        else:
            subprocess.check_call(command, shell=True)

processes = 9
dry_run = False
print(f'# commands: {len(command_pairs)}')
sys.stdout.flush()
with multiprocessing.Pool(processes=processes) as pool:
    workers = []
    for key, command_pair in command_pairs.items():
        commands = []
        metacal_products = gen2_products.metacal_outputs(*key)
        for product, command in zip(metacal_products, command_pair):
            if not os.path.isfile(product):
                commands.append(command)
        workers.append(pool.apply_async(run_cls, commands,
                                        dict(dry_run=dry_run)))
    pool.close()
    pool.join()
    _ = [worker.get() for worker in workers]

