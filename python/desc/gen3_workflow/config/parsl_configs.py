"""
Module to perform runtime-specified loading of the parsl config.
"""
import logging
import parsl
from parsl.executors import WorkQueueExecutor, ThreadPoolExecutor
from parsl.providers import LocalProvider, SlurmProvider
from parsl.launchers import SrunLauncher
from parsl.monitoring.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname
from parsl.utils import get_all_checkpoints
import lsst.utils


__all__ = ['load_parsl_config']


def slurm_provider(nodes_per_block=1, constraint='knl', qos='regular',
                   walltime='10:00:00', time_min=None, **other_options):
    """Factory function to provide a SlurmProvider for running at NERSC."""
    # Guard against empty string or None values for required keywords.
    if not nodes_per_block:
        nodes_per_block = 1
    if not constraint:
        constraint = 'knl'
    if not qos:
        qos = 'regular'
    if not walltime:
        walltime = '10:00:00'
    scheduler_options = (f'#SBATCH --constraint={constraint}\n'
                         f'#SBATCH --qos={qos}\n'
                         '#SBATCH --module=cvmfs\n'
                         '#SBATCH -L cvmfs')
    if time_min:
        scheduler_options = '\n'.join((scheduler_options,
                                       f'#SBATCH --time-min={time_min}'))
    provider_options = dict(walltime=walltime,
                            scheduler_options=scheduler_options,
                            nodes_per_block=nodes_per_block,
                            exclusive=True,
                            init_blocks=0,
                            min_blocks=0,
                            max_blocks=1,
                            parallelism=0,
                            launcher=SrunLauncher(
                                overrides='-K0 -k --slurmd-debug=verbose'),
                            cmd_timeout=300)
    provider_options.update(other_options)
    return SlurmProvider('None', **provider_options)


def set_config_options(retries, monitoring, workflow_name, checkpoint):
    """
    Package retries, monitoring, and checkpoint options for
    parsl.config.Config as a dict.
    """
    config_options = {'retries': retries}
    if monitoring:
        config_options['monitoring'] \
            = MonitoringHub(hub_address=address_by_hostname(),
                            hub_port=55055,
                            monitoring_debug=False,
                            resource_monitoring_interval=60,
                            workflow_name=workflow_name)
    if checkpoint:
        config_options['checkpoint_mode'] = 'task_exit'
        config_options['checkpoint_files'] = get_all_checkpoints()
    return config_options


def workqueue_config(provider, monitoring=False, workflow_name=None,
                     checkpoint=False,  retries=1, worker_options="",
                     log_level=logging.DEBUG, wq_max_retries=1):
    """Load a parsl config for a WorkQueueExecutor and the supplied provider."""
    logger = logging.getLogger("parsl.executors.workqueue.executor")
    logger.setLevel(log_level)

    executors = [WorkQueueExecutor(label='work_queue', port=9000,
                                   shared_fs=True, provider=provider,
                                   worker_options=worker_options,
                                   autolabel=False,
                                   max_retries=wq_max_retries),
                 ThreadPoolExecutor(max_threads=1, label='submit-node')]

    config_options = set_config_options(retries, monitoring, workflow_name,
                                        checkpoint)

    config = parsl.config.Config(strategy='simple',
                                 garbage_collect=False,
                                 app_cache=True,
                                 executors=executors,
                                 **config_options)
    return parsl.load(config)


def thread_pool_config(max_threads, monitoring=False, workflow_name=None,
                       checkpoint=False, retries=1,
                       labels=('submit-node', 'batch-small',
                               'batch-medium', 'batch-large')):
    """Load a parsl config using ThreadPoolExecutor."""
    executors = [ThreadPoolExecutor(max_threads=max_threads, label=label)
                 for label in labels]
    config_options = set_config_options(retries, monitoring, workflow_name,
                                        checkpoint)
    config = parsl.config.Config(executors=executors, **config_options)
    return parsl.load(config)


def load_parsl_config(bps_config):
    """Load the parsl config using the options in bps_config."""
    # Load a module-based config.
    if not bps_config['parsl_config']:
        return lsst.utils.doImport(bps_config['parslConfig']).DFK

    # Load using a runtime-configurable parsl config.
    config = bps_config['parsl_config']
    retries = config['retries'] if config['retries'] else 0
    workflow_name = (config['workflow_name'] if config['workflow_name']
                     else bps_config['outCollection'])

    if config['executor'] == 'ThreadPool':
        return thread_pool_config(config['max_threads'],
                                  monitoring=config['monitoring'],
                                  workflow_name=workflow_name,
                                  checkpoint=config['checkpoint'],
                                  retries=retries)

    if config['provider'] == 'Slurm':
        provider = slurm_provider(nodes_per_block=config['nodes_per_block'],
                                  constraint=config['constraint'],
                                  qos=config['qos'],
                                  walltime=config['walltime'],
                                  time_min=config['time_min'])
    elif config['provider'] == 'Local':
        provider = LocalProvider(init_blocks=0, min_blocks=0, max_blocks=1)
    else:
        raise RuntimeError("Unknown or unspecified provider in "
                           f"bps config: {config['provider']}")

    if config['executor'] == 'WorkQueue':
        log_level = config['log_level']
        if not log_level:
            log_level = logging.DEBUG
        else:
            log_level = eval(log_level)
        wq_max_retries = config['wq_max_retries']
        if wq_max_retries == 'None':
            wq_max_retries = None
        elif wq_max_retries == '':
            wq_max_retries = 1
        else:
            wq_max_retries = int(wq_max_retries)
        return workqueue_config(provider,
                                monitoring=config['monitoring'],
                                workflow_name=workflow_name,
                                checkpoint=config['checkpoint'],
                                retries=retries,
                                worker_options=config['worker_options'],
                                log_level=log_level,
                                wq_max_retries=wq_max_retries)

    raise RuntimeError("Unknown or unspecified executor in "
                       f"bps config: {config['executor']}")
