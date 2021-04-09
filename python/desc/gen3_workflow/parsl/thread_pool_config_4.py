"""Parsl config for using ThreadPoolExecutor with max_threads=4."""
import parsl
from parsl.executors.threads import ThreadPoolExecutor
executors = [ThreadPoolExecutor(max_threads=4, label=label) for label in
             'batch-small batch-medium batch-large submit-node'.split()]
config = parsl.config.Config(executors=executors, retries=1)
DFK = parsl.load(config)
