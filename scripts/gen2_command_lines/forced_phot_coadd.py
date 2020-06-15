import os
import sys
import subprocess
import multiprocessing
from desc.gen3_workflow import CommandLineGenerator, Gen2Products, \
    missing_outputs

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

forced_phot_coadd_dict = dict()
for patch in patches:
    print(patch)
    sys.stdout.flush()
    forced_phot_coadd_dict.update(cl_gen.get_ftp_cls('forcedPhotCoadd',
                                                     'coaddPhot', tract, patch))

def run_cls(*commands, dry_run=True):
    for command in commands:
        if dry_run:
            print(command)
            print()
        else:
            subprocess.check_call(command, shell=True)

gen2_products = Gen2Products(os.path.join(local_repo, 'rerun', 'coaddPhot'))

dry_run = True

processes = 32
with multiprocessing.Pool(processes=processes) as pool:
    workers = []
    for patch in patches:
        for band in bands:
            key = band, tract, patch
            if not missing_outputs(
                    gen2_products.ftp_task_outputs('forcedPhotCoadd', *key)):
                continue
            workers.append(pool.apply_async(run_cls,
                                            (forced_phot_coadd_dict[key],),
                                            dict(dry_run=dry_run)))
    print("# workers:", len(workers))
    pool.close()
    pool.join()
    _ = [worker.get() for worker in workers]
