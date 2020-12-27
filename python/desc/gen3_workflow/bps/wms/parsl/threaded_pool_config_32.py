"""Parsl config for using ThreadedPoolExecutor with max_threads=32."""
import parsl
from parsl.executors.threads import ThreadPoolExecutor
executors = [ThreadPoolExecutor(max_threads=32, label=label) for label in
             'cori-small cori-medium cori-large submit-node'.split()]
config = parsl.config.Config(executors=executors, retries=1)
DFK = parsl.load(config)
