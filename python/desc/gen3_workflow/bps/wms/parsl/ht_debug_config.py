"""
Parsl config to use a Haswell node on the debug queue.
"""
from desc.gen3_workflow.bps.wms.parsl.htxFactory import HtxFactory
import parsl
from parsl.executors.threads import ThreadPoolExecutor

htx_factory = HtxFactory()

cori_small = htx_factory.create(label='cori-small',
                                arch='haswell',
                                qos='debug',
                                max_workers=25,
                                walltime='0:30:00')

cori_medium = htx_factory.create(label='cori-medium',
                                 arch='haswell',
                                 qos='debug',
                                 max_workers=25,
                                 walltime='0:30:00')

cori_large = htx_factory.create(label='cori-large',
                                arch='haswell',
                                qos='debug',
                                max_workers=25,
                                walltime='0:30:00')

local_executor = ThreadPoolExecutor(max_threads=4, label="submit-node")

config = parsl.config.Config(executors=[cori_small, cori_medium, cori_large,
                                        local_executor],
                             app_cache=True,
                             retries=1)
DFK = parsl.load(config)
