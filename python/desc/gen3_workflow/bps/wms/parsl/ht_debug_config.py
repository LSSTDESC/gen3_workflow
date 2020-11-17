"""
Parsl config to use a Haswell node on the debug queue.
"""
import os
import parsl
from parsl.addresses import address_by_hostname
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider


scheduler_options = """#SBATCH --constraint=haswell
#SBATCH --qos=debug
#SBATCH --module=cvmfs
#SBATCH -L cvmfs
"""

haswell_debug = HighThroughputExecutor(
    label='haswell_debug',
    address=address_by_hostname(),
    worker_debug=False,
    max_workers=25,               ## workers(user tasks)/node
    heartbeat_period=60,
    heartbeat_threshold=180,   # time-out betweeen batch and local nodes
    provider=SlurmProvider(
        "None",                # cori queue/partition/qos
        nodes_per_block=1,     # nodes per batch job
        exclusive=True,
        init_blocks=0,         # blocks (batch jobs) to start with (on spec)
        min_blocks=0,
        max_blocks=1,          # max # of batch jobs
        parallelism=0,         # >0 causes multiple batch jobs
        scheduler_options=scheduler_options,
        launcher=SrunLauncher(overrides='-K0 -k --slurmd-debug=verbose'),
        cmd_timeout=300,       # timeout (sec) for slurm commands
        walltime="0:30:00"
    ),
)

config = parsl.config.Config(executors=[haswell_debug],
                             app_cache=True,
                             retries=1)
DFK = parsl.load(config)
