"""Parsl config for WorkQueueExecutor using a LocalProvider"""
import parsl
from parsl.executors import WorkQueueExecutor, ThreadPoolExecutor
from parsl.providers import LocalProvider
from parsl.monitoring.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname
from parsl.utils import get_all_checkpoints

local_provider = LocalProvider(init_blocks=0, min_blocks=0, max_blocks=1)
executors = [WorkQueueExecutor(label='work_queue', port=9000, shared_fs=True,
                               provider=local_provider, autolabel=False,
                               address=address_by_hostname()),
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
                             checkpoint_mode='task_exit',
                             checkpoint_files=get_all_checkpoints(),
                             retries=1)
DFK = parsl.load(config)
