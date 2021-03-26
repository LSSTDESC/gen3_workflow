"""
Parsl config to use KNL nodes on in the flex queue.
"""
from desc.gen3_workflow.bps.wms.parsl.htxFactory import HtxFactory, \
    PROVIDER_OPTIONS
import parsl
from parsl.executors.threads import ThreadPoolExecutor


scheduler_options_template = ("#SBATCH --constraint={}\n"
                              "#SBATCH --qos={}\n"
                              "#SBATCH --module=cvmfs\n"
                              "#SBATCH -L cvmfs\n"
                              "#SBATCH --time-min 2:00:00")

provider_options = PROVIDER_OPTIONS
provider_options['nodes_per_block'] = 1

htx_factory = HtxFactory(scheduler_options_template=scheduler_options_template,
                         provider_options=provider_options)

qos = 'flex'
batch_small = htx_factory.create(label='batch-small',
                                 arch='knl',
                                 qos=qos,
                                 mem_per_worker=2,
                                 walltime='10:00:00')

batch_medium = htx_factory.create(label='batch-medium',
                                  arch='knl',
                                  qos=qos,
                                  mem_per_worker=4,
                                  walltime='10:00:00')

batch_large = htx_factory.create(label='batch-large',
                                 arch='knl',
                                 qos=qos,
                                 mem_per_worker=8,
                                 walltime='40:00:00')

local_executor = ThreadPoolExecutor(max_threads=1, label="submit-node")

config = parsl.config.Config(executors=[batch_small, batch_medium, batch_large,
                                        local_executor],
                             app_cache=True,
                             retries=1)
DFK = parsl.load(config)
