"""
Parsl config to use a Haswell node on the debug queue.
"""
from desc.gen3_workflow.bps.wms.parsl.htxFactory import HtxFactory
import parsl
from parsl.executors.threads import ThreadPoolExecutor

htx_factory = HtxFactory()

batch_small = htx_factory.create(label='batch-small',
                                 arch='haswell',
                                 qos='debug',
                                 mem_per_worker=4,
                                 walltime='0:30:00')

batch_medium = htx_factory.create(label='batch-medium',
                                  arch='haswell',
                                  qos='debug',
                                  mem_per_worker=4,
                                  walltime='0:30:00')

batch_large = htx_factory.create(label='batch-large',
                                 arch='haswell',
                                 qos='debug',
                                 mem_per_worker=4,
                                 walltime='0:30:00')

local_executor = ThreadPoolExecutor(max_threads=1, label="submit-node")

config = parsl.config.Config(executors=[batch_small, batch_medium, batch_large,
                                        local_executor],
                             app_cache=True,
                             retries=1)
DFK = parsl.load(config)
