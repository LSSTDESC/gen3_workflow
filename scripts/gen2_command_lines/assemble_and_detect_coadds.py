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
patches = set(cl_gen.df0.query(f'tract=={tract}')['patch'])

assemble_coadd_dict = dict()
detect_coadd_dict = dict()
for patch in patches:
    print(patch)
    sys.stdout.flush()
    assemble_coadd_dict.update(cl_gen.get_visit_list_cls('assembleCoadd',
                                                         'coadd', tract, patch))
    detect_coadd_dict.update(cl_gen.get_ftp_cls('detectCoaddSources',
                                                'coadd:coaddPhot',
                                                tract, patch))

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
            commands = []
            if missing_outputs(gen2_products.assembleCoadd_outputs(*key)):
                commands.append(assemble_coadd_dict[key])
            if missing_outputs(
                    gen2_products.ftp_task_outputs('detectCoaddSources', *key)):
                commands.append(detect_coadd_dict[key])
            workers.append(pool.apply_async(run_cls, commands,
                                            dict(dry_run=dry_run)))
    print("# workers:", len(workers))
    pool.close()
    pool.join()
    _ = [worker.get() for worker in workers]
