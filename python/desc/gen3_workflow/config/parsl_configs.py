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
from lsst.ctrl.bps import BpsConfig
import lsst.utils


__all__ = ['load_parsl_config']


def set_parsl_logging(bps_config):
    """Set parsl logging levels."""
    config = dict(bps_config['parsl_config'])
    parsl_log_level = config.get('log_level', 'logging.INFO')
    loggers = [_ for _ in logging.root.manager.loggerDict
               if _.startswith('parsl')]
    log_level = eval(parsl_log_level)
    for logger in loggers:
        logging.getLogger(logger).setLevel(log_level)
    return log_level


def local_provider(nodes_per_block=1, **unused_options):
    """
    Factory function to provide a LocalProvider, with the option to
    set the number of nodes to use.  If nodes_per_block > 1, then
    use `launcher=SrunLauncher(overrides='-K0 -k --slurmd-debug=verbose')`,
    otherwise use the default, `launcher=SingleNodeLauncher()`.
    """
    provider_options = dict(nodes_per_block=nodes_per_block,
                            init_blocks=0,
                            min_blocks=0,
                            max_blocks=1,
                            parallelism=0,
                            cmd_timeout=300)
    if nodes_per_block > 1:
        provider_options['launcher'] \
            = SrunLauncher(overrides='-K0 -k --slurmd-debug=verbose')
    return LocalProvider(**provider_options)


def slurm_provider(nodes_per_block=1, constraint='knl', qos='regular',
                   walltime='10:00:00', time_min=None, **unused_options):
    """Factory function to provide a SlurmProvider for running at NERSC."""
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
    return SlurmProvider('None', **provider_options)


def set_config_options(retries, monitoring, workflow_name, checkpoint,
                       monitoring_debug):
    """
    Package retries, monitoring, and checkpoint options for
    parsl.config.Config as a dict.
    """
    config_options = {'retries': retries}
    if monitoring:
        config_options['monitoring'] \
            = MonitoringHub(hub_address=address_by_hostname(),
                            hub_port=55055,
                            monitoring_debug=monitoring_debug,
                            resource_monitoring_interval=60,
                            workflow_name=workflow_name)
    if checkpoint:
        config_options['checkpoint_mode'] = 'task_exit'
        config_options['checkpoint_files'] = get_all_checkpoints()

    return config_options


def workqueue_config(provider=None, monitoring=False, workflow_name=None,
                     checkpoint=False,  retries=1, worker_options="",
                     wq_max_retries=1, port=9000, monitoring_debug=False,
                     **unused_options):
    """Load a parsl config for a WorkQueueExecutor and the supplied provider."""
    executors = [WorkQueueExecutor(label='work_queue', port=port,
                                   shared_fs=True, provider=provider,
                                   worker_options=worker_options,
                                   autolabel=False,
                                   max_retries=wq_max_retries),
                 ThreadPoolExecutor(max_threads=1, label='submit-node')]

    config_options = set_config_options(retries, monitoring, workflow_name,
                                        checkpoint, monitoring_debug)

    config = parsl.config.Config(strategy='simple',
                                 garbage_collect=False,
                                 app_cache=True,
                                 executors=executors,
                                 **config_options)
    return parsl.load(config)


def thread_pool_config(max_threads=1, monitoring=False, workflow_name=None,
                       checkpoint=False, retries=1,
                       labels=('submit-node', 'batch-small',
                               'batch-medium', 'batch-large'),
                       monitoring_debug=False,
                       **unused_options):
    """Load a parsl config using ThreadPoolExecutor."""
    executors = [ThreadPoolExecutor(max_threads=max_threads, label=label)
                 for label in labels]
    config_options = set_config_options(retries, monitoring, workflow_name,
                                        checkpoint, monitoring_debug)
    config = parsl.config.Config(executors=executors, **config_options)
    return parsl.load(config)


def load_parsl_config(bps_config):
    """Load the parsl config using the options in bps_config."""
    log_level = set_parsl_logging(bps_config)

    # Handle the case where parslConfig is set to a config module.
    if not isinstance(bps_config['parslConfig'], BpsConfig):
        return lsst.utils.doImport(bps_config['parslConfig']).DFK

    # Load using a runtime-configurable parsl config.
    #
    # Cast the BpsConfig object as a Python dict to disable the
    # behavior of BpsConfig to return an empty string for missing keys
    # and to provide the dict.get method for handling default values.
    config = dict(bps_config['parsl_config'])
    config['monitoring_debug'] = (log_level == logging.DEBUG)
    config['retries'] = config.get('retries', 0)
    config['workflow_name'] \
        = config.get('workflow_name', bps_config['outputRun'])

    if config['executor'] == 'ThreadPool':
        return thread_pool_config(**config)

    if config['provider'] == 'Slurm':
        config['provider'] = slurm_provider(**config)
    elif config['provider'] == 'Local':
        config['provider'] = local_provider(**config)
    else:
        raise RuntimeError("Unknown or unspecified provider in "
                           f"bps config: {config['provider']}")

    if config['executor'] == 'WorkQueue':
        try:
            config['wq_max_retries'] = int(config.get('wq_max_retries', 1))
        except ValueError:
            config['wq_max_retries'] = None
        return workqueue_config(**config)

    raise RuntimeError("Unknown or unspecified executor in "
                       f"bps config: {config['executor']}")
