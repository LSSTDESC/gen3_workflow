"""
Parsl-based workflow management service plug-in for ctrl_bps.
"""
import os
from collections import defaultdict
import subprocess
import pandas as pd
import lsst.utils
from lsst.daf.butler import Butler
from lsst.ctrl.bps.wms_service import BaseWmsWorkflow, BaseWmsService
from desc.gen3_workflow.bps.wms.parsl.cori_apps import \
    small_bash_app, medium_bash_app, large_bash_app, local_bash_app
import parsl


def run_command(command_line, inputs=(), stdout=None, stderr=None):
    """
    Run a command line as a parsl.bash_app.
    """
    return command_line


RUN_COMMANDS = dict(small=small_bash_app(run_command),
                    medium=medium_bash_app(run_command),
                    large=large_bash_app(run_command),
                    local=local_bash_app(run_command))


def get_run_command(job):
    """
    Get the run command appropriate for the required resources for the
    specified job.
    """
    task_label = list(job.gwf_job.quantum_graph)[0].taskDef.label
    if task_label in ('assembleCoadd', 'detection', 'deblend', 'measure',
                      'forcedPhotCoadd'):
        job_size = 'large'
    elif task_label.startswith('merge'):
        job_size = 'small'
    else:
        job_size = 'medium'
    print('get_run_command:', task_label, job_size)
    return RUN_COMMANDS[job_size]


@parsl.python_app
def no_op_job():
    """
    A no-op parsl.python_app to return a future for a job that already
    has its outputs.
    """
    return 0


class ParslJob:
    """
    Wrapper class for a GenericWorkflowJob.  This class keeps track of
    prerequisite and dependent jobs, and passes the required input
    jobs as futures to the parsl.bash_app that executes the underlying
    quantum graph.
    """
    def __init__(self, gwf_job, config):
        """
        Parameters
        ----------
        gwf_job: `lsst.ctrl.bps.wms.GenericWorkflowJob`
            Workflow job containing execution information for the
            quantum or task to be run.
        config: `lsst.ctrl.bps.BpsConfig`
            Configuration object with job info.
        """
        self.gwf_job = gwf_job
        self.config = config
        self.dependencies = set()
        self.prereqs = set()
        self._done = False
        self.future = None

    def command_line(self):
        """Return the job command line to run in bash."""
        prefix = self.config.get('commandPrepend')
        return ' '.join([prefix, self.gwf_job.cmdline]) if prefix \
            else self.gwf_job.cmdline

    def add_dependency(self, dependency):
        """
        Add a job dependency based on the workflow DAG.

        Parameters
        ----------
        dependency: `ParslJob`
        """
        self.dependencies.add(dependency)

    def add_prereq(self, prereq):
        """
        Add a job prerequisite based on the workflow DAG.

        Parameters
        ----------
        prereq: `ParslJob`
        """
        self.prereqs.add(prereq)

    @property
    def done(self):
        """
        Execution state of the job based either on runtime futures
        or existence of outputs in the data repo.
        """
        def future_settled(job):
            return (job.future is not None and job.future.done()
                    and job.future.exception() is None)
        if not self._done and (future_settled(self) or self.have_outputs()):
            self._done = True
        return self._done

    def log_files(self):
        """
        Return a dict of filenames for directing stderr and stdout.
        """
        log_dir = os.path.join(self.config['submitPath'], 'logging')
        return dict(stderr=os.path.join(log_dir, f'{self.gwf_job.name}.stderr'))

    def get_future(self):
        """
        Get the parsl app future for the job to be run.
        """
        if self.done:
            self.future = no_op_job()
        if self.future is None:
            inputs = [_.get_future() for _ in self.prereqs]
            my_run_command = get_run_command(self)
            self.future = my_run_command(self.command_line(), inputs=inputs,
                                         **self.log_files())
        return self.future

    def have_outputs(self):
        """
        Use the repo butler to determine if a job's outputs are present.
        If any outputs are missing, return False.
        """
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
    """
    Class to generate ParslJob objects with dependencies specified in
    the generic_worklow DAG.  This class also serves as a container
    for all of the jobs in the DAG.
    """
    def __init__(self, generic_workflow, config):
        """
        Parameters
        ----------
        generic_workflow: `lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic representation of a single workflow.
        config: `lsst.ctrl.bps.BpsConfig`
            Configuration of the worklow.
        """
        super().__init__()
        self.gwf = generic_workflow
        self.config = config
        self._pipetaskInit()
        self._ingest()
        self.set_status()

    def _ingest(self):
        """Ingest the workflow as ParslJobs."""
        self.num_visits = defaultdict(dict)
        for job_name in self.gwf:
            if job_name == 'pipetaskInit':
                continue
            job = self.gwf.get_job(job_name)
            if 'assembleCoadd' in job.label:
                warps = (list(job.quantum_graph)[0]
                         .quantum.inputs['deepCoadd_directWarp'])
                tract_patch = warps[0].dataId['tract'], warps[0].dataId['patch']
                band = warps[0].dataId['band']
                self.num_visits[tract_patch][band] = len(warps)
            for successor_job in self.gwf.successors(job_name):
                self[job_name].add_dependency(self[successor_job])
                self[successor_job].add_prereq(self[job_name])

    def set_status(self):
        """Set the pandas dataframe containing the workflow status."""
        self.task_types = []
        data = defaultdict(list)
        for job_name, job in self.items():
            task_type = job_name.split('_')[1]
            data['task_type'].append(task_type)
            if task_type not in self.task_types:
                self.task_types.append(task_type)
            data['job_name'].append(job_name)
            data['status'].append(job.done)
        self.status = pd.DataFrame(data=data)

    def __str__(self):
        """Return a summary of the workflow status."""
        summary = ''
        for task_type in self.task_types:
            my_df = self.status.query(f'task_type == "{task_type}"')
            num_tasks = len(my_df)
            num_pending = len(my_df.query('status == False'))
            summary += f'{task_type:25s}  {num_pending:5d}  {num_tasks:5d}\n'
        return summary

    def __getitem__(self, job_name):
        """
        Re-implementation of dict.__getitem__ to create ParslJobs
        from each of the named jobs in the generic_workflow.
        """
        if not job_name in self:
            gwf_job = self.gwf.get_job(job_name)
            super().__setitem__(job_name, ParslJob(gwf_job, self.config))
        return super().__getitem__(job_name)

    def _pipetaskInit(self):
        """If the output collection isn't in the repo, run pipetaskInit."""
        butler = Butler(self.config['butlerConfig'])
        if self.config['outCollection'] not in \
           butler.registry.queryCollections():
            pipetaskInit = self.gwf.get_job('pipetaskInit')
            command = 'time ' + pipetaskInit.cmdline
            subprocess.check_call(command, shell=True)

    def run(self, block=True):
        """
        Run the encapsulated workflow by requesting the futures
        of the jobs at the endpoints of the DAG.
        """
        futures = [job.get_future() for job in self.values()
                   if not job.dependencies]
        if block:
            # Calling .result() for each future blocks returning from
            # this method until all the jobs have executed.  This is
            # needed for running in a non-interactive python process
            # that would otherwise end before the futures resolve.
            _ = [future.result() for future in futures]


class ParslService(BaseWmsService):
    """Parsl-based implementation for the WMS interface."""
    def prepare(self, config, generic_workflow, out_prefix=None):
        """
        Convert a generic workflow to a Parsl pipeline.

        Parameters
        ----------
        config: `lss.ctrl.bps.BpsConfig`
            Configuration of the workflow.
        generic_workflow: `lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic representation of a single workflow.
        out_prefix : `str` [None]
            Prefix for WMS output files.

        Returns
        -------
        ParslWorkflow
        """
        # Import the Parsl runtime config.
        try:
            lsst.utils.doImport(config['parslConfig'])
        except RuntimeError:
            pass

        service_class = 'desc.gen3_workflow.bps.wms.parsl.ParslService'
        workflow = ParslWorkflow.\
            from_generic_workflow(config, generic_workflow, out_prefix,
                                  service_class)

        return workflow

    def submit(self, workflow):
        """
        Submit a workflow for execution.

        Parameters
        ----------
        workflow: `desc.gen3_workflow.bps.wms.parsl_service.ParslWorkflow`
            Workflow object to execute.
        """
        workflow.parsl_graph.run()


class ParslWorkflow(BaseWmsWorkflow):
    """Parsl-based workflow object to manage execution of workflow."""
    def __init__(self, name, config=None):
        """
        Parameters
        ----------
        name: `str`
             Workflow name.
        config: `lsst.ctrl.bps.BpsConfig`
             Workflow config.
        """
        super().__init__(name, config)
        self.parsl_graph = None

    @classmethod
    def from_generic_workflow(cls, config, generic_workflow, out_prefix,
                              service_class):
        """
        Create a ParslWorkflow object from a generic_workflow.

        Parameters
        ----------
        config: `lss.ctrl.bps.BpsConfig`
            Configuration of the workflow.
        generic_workflow: `lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic representation of a single workflow.
        out_prefix: `str`
            Prefix for WMS output files.
        service_class: `str`
            Full module name of WMS service class that created this workflow.

        Returns
        -------
        ParslWorkflow
        """
        parsl_workflow = cls(generic_workflow.name, config)
        parsl_workflow.parsl_graph = ParslGraph(generic_workflow, config)
        parsl_workflow.submit_path = out_prefix
        return parsl_workflow
