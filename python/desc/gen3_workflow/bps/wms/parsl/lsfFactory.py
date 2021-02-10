"""
Factory for creating HighThroughputExecutor objects for running at NERSC.
"""
from parsl.addresses import address_by_hostname
from parsl.executors import HighThroughputExecutor
from parsl.launchers import Launcher
from parsl.providers import LsfProvider


class BsubLauncher(Launcher)
    """  Worker launcher that wraps the user's command with the Jsrun launch framework
    to launch multiple cmd invocations in parallel on a single job allocation
    """
    def __init__(self, debug: bool = True, overrides: str = ''):
        """
        Parameters
        ----------
        overrides: str
             This string will be passed to the JSrun launcher. Default: ''
        """
        super().__init__(debug=debug)
        self.overrides = overrides

    def __call__(self, command, tasks_per_node, nodes_per_block):
        """
        Args:
        - command (string): The command string to be launched
        - tasks_per_node (int) : Workers to launch per node
        - nodes_per_block (int) : Number of nodes in a block
        """

        tasks_per_block = tasks_per_node * nodes_per_block
        debug_num = int(self.debug)

        x = '''set -e
WORKERCOUNT={tasks_per_block}
cat << JSRUN_EOF > cmd_$JOBNAME.sh
{command}
JSRUN_EOF
chmod a+x cmd_$JOBNAME.sh
bsub -n {tasks_per_block} -r {tasks_per_node} {overrides} /bin/bash cmd_$JOBNAME.sh &
wait
[[ "{debug}" == "1" ]] && echo "Done"
'''.format(command=command,
           tasks_per_block=tasks_per_block,
           tasks_per_node=tasks_per_node,
           overrides=self.overrides,
           debug=debug_num)
        return x


HTX_OPTIONS = dict(worker_debug=False,
                   heartbeat_period=60,
                   heartbeat_threshold=180)

PROVIDER_OPTIONS = dict(nodes_per_block=1,
                        exclusive=True,
                        init_blocks=0,
                        min_blocks=0,
                        max_blocks=1,
                        parallelism=0,
                        launcher=BsubLauncher(),
                        cmd_timeout=300)



class LsfFactory:
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
            self.scheduler_options_template = ("#BSUB -R={}\n"
                                               "#BSUB -q={}\n")
        else:
            self.scheduler_options_template = scheduler_options_template

    def create(self, label, arch, qos, mem_per_worker, walltime):
        """Create a HighThroughputExecutor object"""
        scheduler_options = self.scheduler_options_template.format(arch, qos)
        provider = LsfProvider("None", walltime=walltime,
                               scheduler_options=scheduler_options,
                               **self.provider_options)
        return HighThroughputExecutor(label=label,
                                      mem_per_worker=mem_per_worker,
                                      address=address_by_hostname(),
                                      provider=provider, **self.htx_options)
