"""
Parsl-based workflow management service plug-in for ctrl_bps.
"""
import os
import subprocess
import parsl
from parsl.executors.threads import ThreadPoolExecutor
from lsst.daf.butler import Butler
from lsst.ctrl.bps.wms_service import BaseWmsWorkflow, BaseWmsService
try:
    config = parsl.config.Config(executors=[ThreadPoolExecutor(max_threads=4)])
    dfk = parsl.load(config)
except RuntimeError:
    pass


@parsl.bash_app
def run_job(job, inputs=(), stdout=None, stderr=None):
    """
    Run the quantum graph associated with the specified job.  If the
    job is already done, return an empty string as a no-op command
    line.
    """
    if job.done:
        return ''
    return job.command_line()


class ParslJob:
    """
    Wrapper class for a GenericWorkflowJob.  This class keeps track of
    prerequisite and dependent jobs, and passes the required input
    jobs as futures to the parsl.bash_app that executes the underlying
    quantum graph.
    """
    def __init__(self, gwf_job, config):
        self.gwf_job = gwf_job
        self.config = config
        self.dependencies = set()
        self.prereqs = dict()
        self._done = False
        self.future = None

    def command_line(self):
        return 'time ' + self.gwf_job.cmdline

    def add_dependency(self, dependency):
        self.dependencies.add(dependency)

    def add_prereq(self, prereq):
        self.prereqs[prereq] = False

    def finish(self):
        self._done = True
        for dependency in self.dependencies:
            if self not in dependency.prereqs:
                raise RuntimeError('inconsistent dependency')
            dependency.prereqs[self] = True

    @property
    def done(self):
        def future_settled(job):
            return (job.future is not None and job.future.done()
                    and job.future.exception() is None)
        if not self._done and (future_settled(self) or self.have_outputs()):
            self.finish()
        return self._done

    def log_files(self):
        """
        Return a dict of filenames for directing stderr and stdout.
        """
        log_dir = os.path.join(self.config['submitPath'], 'logging')
        return dict(stderr=os.path.join(log_dir, f'{self.gwf_job.name}.stderr'))

    def get_future(self):
        if self.future is None:
            inputs = [_.get_future() for _ in self.prereqs]
            self.future = run_job(self, inputs=inputs, **self.log_files())
        return self.future

    def have_outputs(self):
        butler = Butler(self.config['butlerConfig'],
                        run=self.config['outCollection'])
        registry = butler.registry
        for node in self.gwf_job.quantum_graph:
            for dataset_refs in node.quantum.outputs.values():
                for dataset_ref in dataset_refs:
                    ref = registry.findDataset(dataset_ref.datasetType,
                                               dataset_ref.dataId,
                                               collections=butler.run)
                    if ref is None:
                        return False
        return True


class ParslGraph(dict):
    def __init__(self, generic_workflow, config):
        super().__init__()
        self.gwf = generic_workflow
        self.config = config
        self.butler = Butler(config['butlerConfig'])
        self._ingest()

    def _ingest(self):
        for job_name in self.gwf:
            if job_name == 'pipetaskInit':
                continue
            for successor_job in self.gwf.successors(job_name):
                self[job_name].add_dependency(self[successor_job])
                self[successor_job].add_prereq(self[job_name])

    def __getitem__(self, job_name):
        if job_name == 'pipetaskInit':
            raise RuntimeError('trying to ingest pipetaskInit')
        if not job_name in self:
            gwf_job = self.gwf.get_job(job_name)
            super().__setitem__(job_name, ParslJob(gwf_job, self.config))
        return super().__getitem__(job_name)


class ParslService(BaseWmsService):

    def prepare(self, config, generic_workflow, out_prefix=None):
        """
        Convert a generic workflow to a Parsl pipeline.
        """
        workflow = ParslWorkflow.from_generic_workflow(config, generic_workflow,
                                                       out_prefix)
        # Run pipetaskInit
        if workflow.parsl_graph.config['outCollection'] not in \
           workflow.parsl_graph.butler.registry.queryCollections():
            pipetaskInit = workflow.parsl_graph.gwf.get_job('pipetaskInit')
            command = 'time ' + pipetaskInit.cmdline
            subprocess.check_call(command, shell=True)

        return workflow

    def submit(self, workflow):
        """
        Submit a workflow for execution.
        """
        # Request the futures of the qgraph jobs in the workflow graph
        # that have no dependencies.  These are the last jobs of the
        # DAG to be executed.  Since the prerequisite futures are set
        # recursively, Parsl will use those futures to schedule all of
        # the jobs in the DAG in a consistent order.
        _ = [job.get_future().result() for job in workflow.parsl_graph.values()
             if not job.dependencies]


class ParslWorkflow(BaseWmsWorkflow):

    def __init__(self, name, config=None):
        super().__init__(name, config)
        self.parsl_graph = None

    @classmethod
    def from_generic_workflow(cls, config, generic_workflow, out_prefix):
        parsl_workflow = cls(generic_workflow.name, config)
        parsl_workflow.parsl_graph = ParslGraph(generic_workflow, config)
        parsl_workflow.submit_path = out_prefix
        return parsl_workflow
