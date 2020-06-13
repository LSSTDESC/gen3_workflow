import os
import subprocess
import multiprocessing
import pandas as pd
import lsst.daf.persistence as dp


def config_file_option(pipe_task):
    configfile = os.path.join(os.environ['OBS_LSST_DIR'], 'config', pipe_task)
    if os.path.isfile(configfile):
        return f'--configfile {configfile} '
    return ''


class CommandLineGenerator:
    def __init__(self, sensor_visit_skymap_file, parent_repo, local_repo,
                 log_dir='.', bands='ugrizy'):
        self.repo = self.setup_local_repo(parent_repo, local_repo)
        self.df0 = pd.read_pickle(sensor_visit_skymap_file)
        self.log_dir = log_dir
        if not os.path.isdir(log_dir):
            os.makedirs(log_dir)
        self.bands = bands

    @staticmethod
    def setup_local_repo(parent_repo, local_repo):
        repo = os.path.abspath(local_repo)
        if not os.path.isdir(repo):
            dp.Butler(inputs=dict(root=parent_repo),
                      outputs=dict(root=repo, mode='rw'))
        return repo

    def get_visits(self, tract, patch, band):
        df = self.df0.query(f'tract=={tract} and patch=="{patch}" '
                            f'and filter=="{band}"')
        return sorted(list(set(df['visit'])))

    def get_per_visit_cls(self, pipe_task, rerun_dir, tract, patch):
        commands = []
        for band in self.bands:
            visits = self.get_visits(tract, patch, band)
            for visit in visits:
                log_name = f'{pipe_task}_{tract}_{patch}_v{visit}-{band}'
                log_file = os.path.join(self.log_dir, f'{log_name}.log')
                mprof_file = os.path.join(self.log_dir, f'mprof_{log_name}.dat')
                prefix = f'time mprof run --output {mprof_file}'
                command = (f'({prefix} {pipe_task}.py {self.repo} '
                           f'--rerun {rerun_dir} --id filter={band} '
                           f'tract={tract} patch={patch} '
                           f'--selectId visit={visit} '
                           f'{config_file_option(f"{pipe_task}.py")}'
                           f'--no-versions --longlog) >& {log_file}')
                commands.append(command)
        return commands

    def get_visit_list_cls(self, pipe_task, rerun_dir, tract, patch):
        commands = []
        for band in self.bands:
            visits = [str(_) for _ in self.get_visits(tract, patch, band)]
            visit_list = '^'.join(visits)
            log_name = f'{pipe_task}_{tract}_{patch}_{band}'
            log_file = os.path.join(self.log_dir, f'{log_name}.log')
            mprof_file = os.path.join(self.log_dir, f'mprof_{log_name}.dat')
            prefix = f'time mprof run --output {mprof_file}'
            command = (f'({prefix} {pipe_task}.py {self.repo} '
                       f'--rerun {rerun_dir} --id filter={band} '
                       f'tract={tract} patch={patch} '
                       f'--selectId visit={visit_list} '
                       f'{config_file_option(f"{pipe_task}.py")}'
                       f'--no-versions --longlog) >& {log_file}')
            commands.append(command)
        return commands

    def get_ftp_cls(self, pipe_task, rerun_dir, tract, patch):
        commands = []
        for band in self.bands:
            log_name = f'{pipe_task}_{tract}_{patch}_{band}'
            log_file = os.path.join(self.log_dir, f'{log_name}.log')
            mprof_file = os.path.join(self.log_dir, f'mprof_{log_name}.dat')
            prefix = f'time mprof run --output {mprof_file}'
            command = (f'({prefix} {pipe_task}.py {self.repo} '
                       f'--rerun {rerun_dir} --id filter={band} '
                       f'tract={tract} patch={patch} '
                       f'{config_file_option(f"{pipe_task}.py")}'
                       f'--no-versions --longlog) >& {log_file}')
            commands.append(command)
        return commands

    def get_merge_task_cl(self, pipe_task, rerun_dir, tract, patch):
        band_list = '^'.join(self.bands)
        log_name = f'{pipe_task}_{tract}_{patch}'
        log_file = os.path.join(self.log_dir, f'{log_name}.log')
        mprof_file = os.path.join(self.log_dir, f'mprof_{log_name}.dat')
        prefix = f'time mprof run --output {mprof_file}'
        command = (f'({prefix} {pipe_task}.py {self.repo} '
                   f'--rerun {rerun_dir} '
                   f'--id filter={band_list} tract={tract} patch={patch} '
                   f'{config_file_option(f"{pipe_task}.py")}'
                   f'--no-versions --longlog) >& {log_file}')
        return command


def dispatch_commands(commands, processes=25, dry_run=False):
    if dry_run:
        for command in commands:
            print(command)
            print()
        return
    with multiprocessing.Pool(processes=processes) as pool:
        workers = []
        for command in commands:
            workers.append(pool.apply_async(subprocess.check_call,
                                            (command,), dict(shell=True)))
        pool.close()
        pool.join()
        _ = [worker.get() for worker in workers]


sensor_visit_skymap_file = 'tracts_mapping_y01.pkl'
parent_repo = ('/global/cfs/cdirs/lsst/production/DC2_ImSim/Run2.2i'
               '/desc_dm_drp/v19.0.0-v1/rerun/run2.2i-calexp-v1')
local_repo = '/global/cscratch1/sd/jchiang8/desc/Run2.2i/DR2/repo_multiproc'
log_dir = 'logging'

bands = 'ugrizy'

cl_gen = CommandLineGenerator(sensor_visit_skymap_file, parent_repo, local_repo,
                              bands=bands, log_dir=log_dir)

tract = 4849
patch = '3,1'

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

dry_run = False

processes = 25
dispatch_commands(make_coadd_temp_exp, processes=processes, dry_run=dry_run)

#processes = 6
#dispatch_commands(assemble_coadd, processes=processes, dry_run=dry_run)

#dispatch_commands(detect_coadd_sources, processes=processes, dry_run=dry_run)

#if dry_run:
#    print(merge_coadd_detections)
#    print()
#else:
#    subprocess.check_call(merge_coadd_detections, shell=True)
#
#dispatch_commands(deblend_coadd_sources, processes=processes, dry_run=dry_run)

#dispatch_commands(measure_coadd_sources, processes=processes, dry_run=dry_run)

#if dry_run:
#    print(merge_coadd_measurements)
#    print()
#else:
#    subprocess.check_call(merge_coadd_measurements, shell=True)
#
#dispatch_commands(forced_phot_coadd, processes=processes, dry_run=dry_run)
