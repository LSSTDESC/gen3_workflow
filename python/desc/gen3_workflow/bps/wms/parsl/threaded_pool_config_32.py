"""Parsl config for using ThreadedPoolExecutor with max_threads=32."""
import parsl
from parsl.executors.threads import ThreadPoolExecutor
config = parsl.config.Config(executors=[ThreadPoolExecutor(max_threads=32)],
                             retries=1)
DFK = parsl.load(config)
