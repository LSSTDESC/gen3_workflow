"""
Parsl config to use a Haswell node on the debug queue.
"""
from desc.gen3_workflow.bps.wms.parsl.htxFactory import HtxFactory
import parsl
from parsl.executors.threads import ThreadPoolExecutor

lsf_factory = LsfFactory()

batch_small = lsf_factory.create(label='batch-small',
                                 arch='lsf',
                                 qos='xlong',
                                 mem_per_worker=1,
                                 walltime='9:00:00')

batch_medium = lsf_factory.create(label='batch-medium',
                                  arch='lsf',
                                  qos='xlong',
                                  mem_per_worker=1,
                                  walltime='10:00:00')

batch_large = lsf_factory.create(label='batch-large',
                                 arch='lsf',
                                 qos='xlong',
                                 mem_per_worker=1,
                                 walltime='40:00:00')

local_executor = ThreadPoolExecutor(max_threads=4, label="submit-node")

config = parsl.config.Config(executors=[batch_small, batch_medium, batch_large,
                                        local_executor],
                             app_cache=True,
                             retries=1)
DFK = parsl.load(config)
