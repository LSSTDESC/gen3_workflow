"""
Parsl config to use KNL nodes in the regular batch queues.
"""
from desc.gen3_workflow.bps.wms.parsl.htxFactory import HtxFactory
import parsl
from parsl.executors.threads import ThreadPoolExecutor

htx_factory = HtxFactory()

batch_small = htx_factory.create(label='batch-small',
                                 arch='knl',
                                 qos='regular',
                                 mem_per_worker=2,
                                 walltime='10:00:00')

batch_medium = htx_factory.create(label='batch-medium',
                                  arch='knl',
                                  qos='regular',
                                  mem_per_worker=4,
                                  walltime='10:00:00')

batch_large = htx_factory.create(label='batch-large',
                                 arch='knl',
                                 qos='regular',
                                 mem_per_worker=8,
                                 walltime='40:00:00')

local_executor = ThreadPoolExecutor(max_threads=1, label="submit-node")

config = parsl.config.Config(executors=[batch_small, batch_medium, batch_large,
                                        local_executor],
                             app_cache=True,
                             retries=1)
DFK = parsl.load(config)
