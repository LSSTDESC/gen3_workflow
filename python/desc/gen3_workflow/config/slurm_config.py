import parsl
from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import SlurmProvider
from parsl.providers import GridEngineProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname
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
              address=address_by_hostname(),
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
                walltime="120:00:00",
                scheduler_options='#SBATCH --mem 3G -L sps',
                cmd_timeout=60,
                ),
              block_error_handler=False
              )

batch_6G = HighThroughputExecutor(
              label='batch-6G',
              address=address_by_hostname(),
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
                walltime="120:00:00",
                scheduler_options='#SBATCH --mem 6G -L sps',
                cmd_timeout=60,
                ),
              block_error_handler=False
              )

batch_8G = HighThroughputExecutor(
              label='batch-8G',
              address=address_by_hostname(),
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
                max_blocks=2000,
                walltime="120:00:00",
                scheduler_options='#SBATCH --mem 8G -L sps',
                cmd_timeout=60,
                ),
              block_error_handler=False
              )

batch_16G = HighThroughputExecutor(
              label='batch-16G',
              address=address_by_hostname(),
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
                max_blocks=1500,
                walltime="120:00:00",
                scheduler_options='#SBATCH --mem 16G -L sps',
                cmd_timeout=60,
                ),
              block_error_handler=False
              )

batch_24G = HighThroughputExecutor(
              label='batch-24G',
              address=address_by_hostname(),
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
                walltime="120:00:00",
                scheduler_options='#SBATCH --mem 24G -L sps',
                cmd_timeout=60,
                ),
              block_error_handler=False
              )

batch_60G = HighThroughputExecutor(
              label='batch-60G',
              address=address_by_hostname(),
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
                walltime="120:00:00",
                scheduler_options='#SBATCH --mem 60G -L sps',
                cmd_timeout=60,
                ),
              block_error_handler=False
              )

batch_180G = HighThroughputExecutor(
              label='batch-180G',
              address=address_by_hostname(),
              worker_debug=False,
              poll_period=1000,
              worker_port_range=worker_port_range,
              interchange_port_range=interchange_port_range,
              max_workers=1,
              provider=SlurmProvider(
                partition='htc_highmem',
                channel=LocalChannel(),
                account='lsst',
                exclusive=False,
                nodes_per_block=1,
                init_blocks=0,
                max_blocks=8,
                walltime="120:00:00",
                scheduler_options='#SBATCH --mem 180G -L sps',
                cmd_timeout=60,
                ),
              block_error_handler=False
              )


local_executor = ThreadPoolExecutor(max_threads=16, label="submit-node")

monitor = MonitoringHub(
       hub_address=address_by_hostname(),
#       hub_port=hub_port,
       logging_endpoint=logging_endpoint,
       monitoring_debug=False,
       resource_monitoring_enabled=True,
       resource_monitoring_interval=600,
   )


config = parsl.config.Config(executors=[batch_3G, batch_6G, batch_8G, batch_16G, batch_24G, batch_60G, batch_180G, local_executor],
                             app_cache=True,
                             retries=2,
                             strategy='htex_auto_scale',
                             run_dir=subrun_dir,
#                             monitoring=monitor)
                             )

DFK = parsl.load(config)

