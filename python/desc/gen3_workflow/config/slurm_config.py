import parsl
from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import SlurmProvider
from parsl.providers import GridEngineProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_query
from parsl.executors.threads import ThreadPoolExecutor
from parsl.monitoring.monitoring import MonitoringHub

from parsl.dataflow.rundirs import make_rundir

import os

run_dir=os.path.join(os.environ['DP02_DIR'],'runinfo')

subrun_dir = make_rundir(run_dir)
logging_endpoint = "sqlite:///" + subrun_dir + "/monitoring.db"

worker_port_range=(54000, 55000)
interchange_port_range=(55001, 56000)

batch_3G = HighThroughputExecutor(
              label='batch-3G',
              address=address_by_query(),
              worker_debug=False,
              poll_period=1000,
              worker_port_range=worker_port_range,
              interchange_port_range=interchange_port_range,
              max_workers=1,
              provider=SlurmProvider(
                partition='htc',
                channel=LocalChannel(),
                account='lsst',
                exclusive=False,
                nodes_per_block=1,
                init_blocks=0,
                max_blocks=3000,
                walltime="168:00:00",
                scheduler_options='#SBATCH --mem 3G -L sps',
                ),
              block_error_handler=False
              )

batch_6G = HighThroughputExecutor(
              label='batch-6G',
              address=address_by_query(),
              worker_debug=False,
              poll_period=1000,
              worker_port_range=worker_port_range,
              interchange_port_range=interchange_port_range,
              max_workers=1,
              provider=SlurmProvider(
                partition='htc',
                channel=LocalChannel(),
                account='lsst',
                exclusive=False,
                nodes_per_block=1,
                init_blocks=0,
                max_blocks=3000,
                walltime="168:00:00",
                scheduler_options='#SBATCH --mem 6G -L sps',
                ),
              block_error_handler=False
              )

batch_18G = HighThroughputExecutor(
              label='batch-18G',
              address=address_by_query(),
              worker_debug=False,
              poll_period=1000,
              worker_port_range=worker_port_range,
              interchange_port_range=interchange_port_range,
              max_workers=1,
              provider=SlurmProvider(
                partition='htc',
                channel=LocalChannel(),
                account='lsst',
                exclusive=False,
                nodes_per_block=1,
                init_blocks=0,
                max_blocks=500,
                walltime="168:00:00",
                scheduler_options='#SBATCH --mem 18G -L sps',
                ),
              block_error_handler=False
              )

batch_54G = HighThroughputExecutor(
              label='batch-54G',
              address=address_by_query(),
              worker_debug=False,
              poll_period=1000,
              worker_port_range=worker_port_range,
              interchange_port_range=interchange_port_range,
              max_workers=1,
              provider=SlurmProvider(
                partition='htc',
                channel=LocalChannel(),
                account='lsst',
                exclusive=False,
                nodes_per_block=1,
                init_blocks=0,
                max_blocks=500,
                walltime="168:00:00",
                scheduler_options='#SBATCH --mem 54G -L sps',
                ),
              block_error_handler=False
              )

#batch_120G = HighThroughputExecutor(
#              label='batch-120G',
#              address=address_by_query(),
#              worker_debug=False,
#              poll_period=1000,
#              worker_port_range=(54000, 54050),
#              interchange_port_range=(54051, 54100),
#              max_workers=1,
#              provider=SlurmProvider(
#                partition='htc',
#                channel=LocalChannel(),
#                account='lsst',
#                exclusive=False,
#                nodes_per_block=1,
#                init_blocks=0,
#                max_blocks=100,
#                walltime="144:00:00",
##                scheduler_options='#SBATCH --mem 120G -L sps',
#                scheduler_options='#SBATCH --mem 170G -L sps',
#                worker_init=initenv,     # Input your worker_init if needed
#                ),
#              )


batch_120G = HighThroughputExecutor(
              label='batch-120G',
              address=address_by_query(),
              worker_debug=False,
              poll_period=1000,
              worker_port_range=worker_port_range,
              interchange_port_range=interchange_port_range,
              max_workers=1,
              provider=GridEngineProvider(
                channel=LocalChannel(),
                nodes_per_block=1,
                init_blocks=0,
                max_blocks=50,
                walltime="144:00:00",
                scheduler_options='#$ -P P_lsst -l cvmfs=1,sps=1 -pe multicores 1 -q mc_highmem_huge',
                worker_init='source /pbs/throng/lsst/software/parsl/dp02-tools/env.sh',
                ),
              )



local_executor = ThreadPoolExecutor(max_threads=16, label="submit-node")

monitor = MonitoringHub(
       hub_address=address_by_query(),
#       hub_port=hub_port,
       logging_endpoint=logging_endpoint,
       monitoring_debug=False,
       resource_monitoring_enabled=True,
       resource_monitoring_interval=600,
   )


config = parsl.config.Config(executors=[batch_3G, batch_6G, batch_18G, batch_54G, batch_120G, local_executor],
                             app_cache=True,
                             retries=2,
                             strategy='htex_auto_scale',
                             run_dir=subrun_dir,
                             monitoring=monitor)

DFK = parsl.load(config)

