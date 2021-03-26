"""Parsl config for WorkQueueExecutor using a LocalProvider"""
import parsl
from parsl.executors import WorkQueueExecutor, ThreadPoolExecutor
from parsl.providers import LocalProvider

local_provider = LocalProvider(init_blocks=0, min_blocks=0, max_blocks=1)
executors = [WorkQueueExecutor(label='work_queue', port=9000, shared_fs=True,
                               provider=local_provider, autolabel=False),
             ThreadPoolExecutor(max_threads=1, label='submit-node')]
config = parsl.config.Config(executors=executors, retries=1)
DFK = parsl.load(config)
