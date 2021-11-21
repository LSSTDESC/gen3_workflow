"""Parsl config for WorkQueueExecutor using a SlurmProvider"""
import parsl
from parsl.executors import WorkQueueExecutor, ThreadPoolExecutor
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
from parsl.monitoring.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname
from parsl.utils import get_all_checkpoints

PROVIDER_OPTIONS = dict(nodes_per_block=1,
                        exclusive=True,
                        init_blocks=0,
                        min_blocks=0,
                        max_blocks=1,
                        parallelism=0,
                        launcher=SrunLauncher(
                            overrides='-K0 -k --slurmd-debug=verbose'),
                        cmd_timeout=300)

SCHEDULER_OPTIONS = ("#SBATCH --constraint=knl\n"
                     "#SBATCH --qos=debug\n"
                     "#SBATCH --module=cvmfs\n"
                     "#SBATCH -L cvmfs")

provider = SlurmProvider('None', walltime='0:30:00',
                         scheduler_options=SCHEDULER_OPTIONS,
                         **PROVIDER_OPTIONS)

executors = [WorkQueueExecutor(label='work_queue', port=9000, shared_fs=True,
                               provider=provider, autolabel=False),
             ThreadPoolExecutor(max_threads=1, label='submit-node')]

monitoring = MonitoringHub(hub_address=address_by_hostname(),
                           hub_port=55055,
                           monitoring_debug=False,
                           resource_monitoring_interval=60)

config = parsl.config.Config(strategy='simple',
                             garbage_collect=False,
                             app_cache=True,
                             executors=executors,
                             monitoring=monitoring,
                             retries=1)
DFK = parsl.load(config)
