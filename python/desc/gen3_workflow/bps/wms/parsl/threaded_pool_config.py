import parsl
from parsl.executors.threads import ThreadPoolExecutor
config = parsl.config.Config(executors=[ThreadPoolExecutor(max_threads=4)])
dfk = parsl.load(config)
