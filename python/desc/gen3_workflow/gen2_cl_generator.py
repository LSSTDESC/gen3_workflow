"""
Module to generate gen2 pipe task command lines.
"""
import os
import subprocess
import multiprocessing
import pandas as pd
import lsst.daf.persistence as dp


__all__ = ['CommandLineGenerator', 'dispatch_commands']


def config_file_option(pipe_task, package_dir=None):
    """
    Produce the `--configfile ...` option for the specified pipe_task
    using the config file in obs_lsst/config.
    """
    if package_dir is None:
        package_dir = os.environ['OBS_LSST_DIR']
    configfile = os.path.join(package_dir, 'config', pipe_task)
    if os.path.isfile(configfile):
        return f'--configfile {configfile} '
    return ''


class CommandLineGenerator:
    """
    Class to generate command lines for gen2 pipe tasks.
    """
    def __init__(self, sensor_visit_skymap_file, parent_repo, local_repo,
                 log_dir='.', bands='ugrizy'):
        """
        Parameters
        ----------
        sensor_visit_skymap_file: str
            Pickle file containing a dataframe of the mappings of
            sensor-visits to patches.
        parent_repo: str
            Parent repo containing the raw files or visit-level calexps
        local_repo: str
            A local repo to contain the processing outputs under the
            rerun directory.
        log_dir: str ['.']
            Directory to contain log files and mprof files.
        bands: str ['ugrizy']
            Bands to process.
        """
        self.repo = self.setup_local_repo(parent_repo, local_repo)
        self.df0 = pd.read_pickle(sensor_visit_skymap_file)
        self.log_dir = log_dir
        if not os.path.isdir(log_dir):
            os.makedirs(log_dir)
        self.bands = bands

    @staticmethod
    def setup_local_repo(parent_repo, local_repo):
        """
        Set up the local repo, relative to the parent repo, to
        contain the pipe task outputs.

        Parameters
        ----------
        parent_repo: str
            Parent repo containing the raw files or visit-level calexps
        local_repo: str
            A local repo to contain the processing outputs under the
            rerun directory.

        Returns
        -------
        full path to the local repo
        """
        repo = os.path.abspath(local_repo)
        if not os.path.isdir(repo):
            dp.Butler(inputs=dict(root=parent_repo),
                      outputs=dict(root=repo, mode='rw'))
        return repo

    def get_visits(self, tract, patch, band):
        """
        Return a sorted list of visits that overlap the specified tract
        and patch for the specified band.
        """
        df = self.df0.query(f'tract=={tract} and patch=="{patch}" '
                            f'and filter=="{band}"')
        return sorted(list(set(df['visit'])))

    def get_per_visit_cls(self, pipe_task, rerun_dir, tract, patch):
        """
        Return the per-visit command lines for the specified pipe task,
        tract, and patch.  Currently only used for `makeCoaddTempExp`.
        """
        commands = dict()
        for band in self.bands:
            visits = self.get_visits(tract, patch, band)
            for visit in visits:
                key = band, tract, patch, visit
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
                commands[key] = command
        return commands

    def get_visit_list_cls(self, pipe_task, rerun_dir, tract, patch):
        """
        Return the per-band command lines, selecting all of the relevant
        visits for the specified pipe task, tract, and patch.  Currently
        only used for `assembleCoadd`.
        """
        commands = dict()
        for band in self.bands:
            key = band, tract, patch
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
            commands[key] = command
        return commands

    def get_ftp_cls(self, pipe_task, rerun_dir, tract, patch):
        """
        Return the per-band command lines intended to operate on the
        coadded patch-level data for the specified pipe task, tract,
        and patch.  Used for `detectCoaddSources`, `deblendCoaddSources`,
        `measureCoaddSources`, and `forcedPhotCoadd`.
        """
        commands = dict()
        for band in self.bands:
            key = band, tract, patch
            log_name = f'{pipe_task}_{tract}_{patch}_{band}'
            log_file = os.path.join(self.log_dir, f'{log_name}.log')
            mprof_file = os.path.join(self.log_dir, f'mprof_{log_name}.dat')
            prefix = f'time mprof run --output {mprof_file}'
            command = (f'({prefix} {pipe_task}.py {self.repo} '
                       f'--rerun {rerun_dir} --id filter={band} '
                       f'tract={tract} patch={patch} '
                       f'{config_file_option(f"{pipe_task}.py")}'
                       f'--no-versions --longlog) >& {log_file}')
            commands[key] = command
        return commands

    def get_merge_task_cl(self, pipe_task, rerun_dir, tract, patch):
        """
        Return the pipe task command lines that merge either detections or
        measurements for the desired tract and patch.  Used for
        `mergeCoaddDetections` and `mergeCoaddMeasurements`.
        """
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

    def get_metacal_cls(self, rerun_dir, tract, patch, filters='g^r^i^z',
                        ipp_dir=None):
        if ipp_dir is None:
            ipp_dir = '/global/u1/j/jchiang8/dev/ImageProcessingPipelines'
        filter_config = os.path.join(ipp_dir, 'config', 'mcal-filters.py')
        task_names = ('mcalmax', 'ngmix')
        pipe_tasks = ('processDeblendedCoaddsMetacalMax',
                      'processDeblendedCoaddsNGMixMax')
        task_configs = ('ngmix-deblended-mcalmax.py',
                        'ngmix-deblended-bd.py')
        commands = []
        for task_name, pipe_task, task_config in zip(task_names, pipe_tasks,
                                                     task_configs):
            log_name = f'{pipe_task}_{tract}_{patch}'
            log_file = os.path.join(self.log_dir, f'{log_name}.log')
            mprof_file = os.path.join(self.log_dir, f'mprof_{log_name}.dat')
            prefix = f'time mprof run --output {mprof_file}'
            ipp_config = os.path.join(ipp_dir, 'config', task_config)
            command = (f'({prefix} {pipe_task}.py {self.repo} '
                       f'--rerun {rerun_dir} '
                       f'--id filter={filters} tract={tract} patch={patch} '
                       f'--configfile {filter_config} '
                       f'--configfile {ipp_config} '
                       f'--no-versions --longlog) >& {log_file}')
            commands.append(command)
        return {(tract, patch): commands}

def dispatch_commands(commands, processes=25, dry_run=False):
    """
    Run the list of command lines asynchronously using the
    multprocessing module for the specified number of processes.
    """
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
