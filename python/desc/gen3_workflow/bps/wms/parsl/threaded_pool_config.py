"""Parsl config for using ThreadedPoolExecutor with max_threads=4."""
import parsl
from parsl.executors.threads import ThreadPoolExecutor
config = parsl.config.Config(executors=[ThreadPoolExecutor(max_threads=4)])
DFK = parsl.load(config)
