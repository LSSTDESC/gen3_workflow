"""
Factory for creating HighThroughputExecutor objects for running at NERSC.
"""
from parsl.addresses import address_by_hostname
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider


HTX_OPTIONS = dict(worker_debug=False,
                   heartbeat_period=60,
                   heartbeat_threshold=180)

PROVIDER_OPTIONS = dict(nodes_per_block=1,
                        exclusive=True,
                        init_blocks=0,
                        min_blocks=0,
                        max_blocks=1,
                        parallelism=0,
                        launcher=SrunLauncher(
                            overrides='-K0 -k --slurmd-debug=verbose'),
                        cmd_timeout=300)


class HtxFactory:
    """
    Factory class to create HighThroughputExecutor objects for running
    in NERSC queues.
    """
    def __init__(self, htx_options=None, provider_options=None,
                 scheduler_options_template=None):
        self.htx_options = HTX_OPTIONS if htx_options is None else htx_options
        self.provider_options = (PROVIDER_OPTIONS if provider_options is None
                                 else provider_options)
        if scheduler_options_template is None:
            self.scheduler_options_template = ("#SBATCH --constraint={}\n"
                                               "#SBATCH --qos={}\n"
                                               "#SBATCH --module=cvmfs\n"
                                               "#SBATCH -L cvmfs")
        else:
            self.scheduler_options_template = scheduler_options_template

    def create(self, label, arch, qos, mem_per_worker, walltime):
        """Create a HighThroughputExecutor object"""
        scheduler_options = self.scheduler_options_template.format(arch, qos)
        provider = SlurmProvider("None", walltime=walltime,
                                 scheduler_options=scheduler_options,
                                 **self.provider_options)
        return HighThroughputExecutor(label=label,
                                      mem_per_worker=mem_per_worker,
                                      address=address_by_hostname(),
                                      provider=provider, **self.htx_options)
