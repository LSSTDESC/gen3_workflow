import os
import subprocess
import pandas as pd
import lsst.daf.persistence as dp


def config_file_option(pipe_task):
    configfile = os.path.join(os.environ['OBS_LSST_DIR'], 'config', pipe_task)
    if os.path.isfile(configfile):
        return f'--configfile {configfile} '
    return ''


class CommandLineGenerator:
    def __init__(self, sensor_visit_skymap_file, base_repo, local_repo,
                 bands='ugrizy'):
        self.repo = self.setup_local_repo(base_repo, local_repo)
        self.df0 = pd.read_pickle(sensor_visit_skymap_file)
        self.bands = bands

    @staticmethod
    def setup_local_repo(base_repo, local_repo):
        repo = os.path.abspath(local_repo)
        if not os.path.isdir(repo):
            dp.Butler(inputs=dict(root=base_repo),
                      outputs=dict(root=repo, mode='rw'))
        return repo

    def get_band_level_task_cl(self, pipe_task, rerun_dir, tract, patch,
                               select_visits=False):
        commands = dict()
        for band in self.bands:
            log_file = f'{pipe_task[:-len(".py")]}_{band}-band.log'
            commands[band] = (f'(time mprof run {pipe_task} {self.repo} '
                              f'--rerun {rerun_dir} --id filter={band} '
                              f'tract={tract} patch={patch} ')
            if select_visits:
                df = self.df0.query(f'tract=={tract} and patch=="{patch}" '
                                    f'and band=="{band}"')
                visits = sorted(list(set(df['visit'])))
                visit_list = '^'.join([str(_) for _ in visits])
                commands[band] += f'--selectId visit={visit_list} '
            commands[band] += (f'{config_file_option(f"{pipe_task}")}'
                               f'--no-versions --longlog) >& {log_file}\n')
            print(commands[band])
            subprocess.check_call(commands[band], shell=True)
        return commands

    def get_merge_task_cl(self, pipe_task, rerun_dir, tract, patch):
        band_list = '^'.join(self.bands)
        log_file = f'{pipe_task[:-len(".py")]}.log'
        command = (f'(time mprof run {pipe_task} {self.repo} '
                   f'--rerun {rerun_dir} '
                   f'--id filter={band_list} tract={tract} patch={patch} '
                   f'{config_file_option(f"{pipe_task}")}'
                   f'--no-versions --longlog) >& {log_file}\n')
        print(command)
        subprocess.check_call(command, shell=True)
        return command

sensor_visit_skymap_file = 'sky_map_df_y01.pkl'
base_repo = ('/global/cfs/cdirs/lsst/production/DC2_ImSim/Run2.2i'
             '/desc_dm_drp/v19.0.0-v1/rerun/run2.2i-calexp-v1')
local_repo = '/global/cscratch1/sd/jchiang8/desc/Run2.2i/DR2/repo'

bands = 'ugrizy'

cl_gen = CommandLineGenerator(sensor_visit_skymap_file, base_repo, local_repo,
                              bands=bands)

tract = 3828
patch = '0,0'

rerun_dir = 'coadd'
for pipe_task in ('makeCoaddTempExp.py', 'assembleCoadd.py'):
    commands = cl_gen.get_band_level_task_cl(pipe_task, rerun_dir, tract, patch,
                                             select_visits=True)

rerun_dir = 'coadd:coaddPhot'
commands = cl_gen.get_band_level_task_cl('detectCoaddSources.py',
                                         rerun_dir, tract, patch)

rerun_dir = 'coaddPhot'
command = cl_gen.get_merge_task_cl('mergeCoaddDetections.py', rerun_dir,
                                   tract, patch)

for pipe_task in ('deblendCoaddSources.py', 'measureCoaddSources.py'):
    commands = cl_gen.get_band_level_task_cl(pipe_task, rerun_dir, tract, patch)

command = cl_gen.get_merge_task_cl('mergeCoaddMeasurements.py', rerun_dir,
                                   tract, patch)

commands = cl_gen.get_band_level_task_cl('forcedPhotCoadd.py', rerun_dir,
                                         tract, patch)
