"""
Parsl config to use a Haswell node on the debug queue.
"""
from desc.gen3_workflow.bps.wms.parsl.htxFactory import HtxFactory
import parsl
from parsl.executors.threads import ThreadPoolExecutor

htx_factory = HtxFactory()

cori_small = htx_factory.create(label='cori-small',
                                arch='knl',
                                qos='regular',
                                mem_per_worker=2,
                                walltime='9:00:00')

cori_medium = htx_factory.create(label='cori-medium',
                                 arch='knl',
                                 qos='regular',
                                 mem_per_worker=2,
                                 walltime='10:00:00')

cori_large = htx_factory.create(label='cori-large',
                                arch='knl',
                                qos='regular',
                                mem_per_worker=8,
                                walltime='40:00:00')

local_executor = ThreadPoolExecutor(max_threads=4, label="submit-node")

config = parsl.config.Config(executors=[cori_small, cori_medium, cori_large,
                                        local_executor],
                             app_cache=True,
                             retries=1)
DFK = parsl.load(config)
