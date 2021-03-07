"""
Parsl-based workflow management service plug-in for ctrl_bps.
"""
import os
from collections import defaultdict
import pickle
import subprocess
import pandas as pd
import lsst.utils
import lsst.daf.butler
from lsst.daf.butler import Butler
from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.submit import BPS_SEARCH_ORDER, create_submission
from lsst.ctrl.bps.wms_service import BaseWmsWorkflow, BaseWmsService
from desc.gen3_workflow.bps.wms.parsl.bash_apps import \
    small_bash_app, medium_bash_app, large_bash_app, local_bash_app
import parsl


__all__ = ['start_pipeline', 'ParslGraph', 'ParslJob']


def start_pipeline(config_file):
    """
    Function to submit a pipeline job using ctrl_bps and the
    Parsl-based plugin.  The returned ParslGraph object provides
    access to the underlying GenericWorkflowJob objects and
    piplinetask quanta and a means of controlling and introspecting
    their execution.

    Parameters
    ----------
    config_file: str
        ctrl_bps yaml config file.

    Returns
    -------
    ParslGraph
    """
    config = BpsConfig(config_file, BPS_SEARCH_ORDER)
    workflow = create_submission(config)
    return workflow.parsl_graph


# Job status values
_PENDING = 'pending'
_RUNNING = 'running'
_SUCCEEDED = 'succeeded'
_FAILED = 'failed'

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
    elif task_label in ('makeWarp', 'mergeDetections', 'mergeMeasurements'):
        job_size = 'medium'
    else:
        job_size = 'small'
    return RUN_COMMANDS[job_size]


@parsl.python_app(executors=['submit-node'])
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
        self._status = _PENDING
        self.future = None

    def command_line(self):
        """Return the job command line to run in bash."""
        command = (self.gwf_job.cmdline +
                   ' && >&2 echo success || >&2 echo failure')
        prefix = self.config.get('commandPrepend')
        if prefix:
            command = ' '.join([prefix, command])
        return command

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
        Execution state of the job based whether the job has
        written the 'success' string to the end of its log file.
        """
        if not self._done:
            self._done = (self.status == _SUCCEEDED)
        return self._done

    @property
    def status(self):
        """Return the job status, either _PENDING, _RUNNING, _SUCCEEDED, or
        _FAILED."""
        if not hasattr(self, '_status'):
            # handle older pickled ParslGraphs
            self._status = _PENDING

        if self._status in (_SUCCEEDED, _FAILED):
            return self._status

        # Check log file.
        log_file = self.log_files()['stderr']
        if os.path.isfile(log_file):
            self._status = _RUNNING
            with open(log_file) as fd:
                outcome = fd.readlines()[-1]
            if 'success' in outcome:
                self._status = _SUCCEEDED
            elif 'failure' in outcome:
                # Guard against failures caused by dataID/datasetType
                # insertion conflicts in the registry db that arise
                # from quanta that succeed but fail to write the
                # "success" string to the log file before the batch
                # allocation times out.  Do this by checking for the
                # job outputs.
                if self.have_outputs():
                    self._status = _SUCCEEDED
                else:
                    self._status = _FAILED

        return self._status

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
    def __init__(self, generic_workflow, config, do_init=True):
        """
        Parameters
        ----------
        generic_workflow: `lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic representation of a single workflow.
        config: `lsst.ctrl.bps.BpsConfig`
            Configuration of the worklow.
        do_init: bool [True]
            Flag to run pipetaskInit.
        """
        super().__init__()
        self.gwf = generic_workflow
        self.config = config
        if do_init:
            self._pipetaskInit()
        self._ingest()
        self._update_status()

    def _ingest(self):
        """Ingest the workflow as ParslJobs."""
        self.num_visits = defaultdict(dict)
        for job_name in self.gwf:
            if job_name == 'pipetaskInit':
                continue
            # Make sure pipelines without downstream dependencies are
            # ingested into the ParslGraph.
            _ = self[job_name]
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

    def _update_status(self):
        """Update the pandas dataframe containing the workflow status."""
        self.task_types = []
        data = defaultdict(list)
        for job_name, job in self.items():
            task_type = job_name.split('_')[1]
            data['task_type'].append(task_type)
            if task_type not in self.task_types:
                self.task_types.append(task_type)
            data['job_name'].append(job_name)
            data['status'].append(job.status)
        self._status_df = pd.DataFrame(data=data)

    @property
    def status(self):
        """Return a summary of the workflow status."""
        self._update_status()
        summary = ['task type                   '
                   'pending  running  succeeded  failed  total\n']
        for task_type in self.task_types:
            my_df = self._status_df.query(f'task_type == "{task_type}"')
            num_tasks = len(my_df)
            num_pending = len(my_df.query(f'status == "{_PENDING}"'))
            num_running = len(my_df.query(f'status == "{_RUNNING}"'))
            num_succeeded = len(my_df.query(f'status == "{_SUCCEEDED}"'))
            num_failed = len(my_df.query(f'status == "{_FAILED}"'))
            summary.append(f'{task_type:25s}     {num_pending:5d}    '
                           f'{num_running:5d}      {num_succeeded:5d}   '
                           f'{num_failed:5d}  {num_tasks:5d}')
        return '\n'.join(summary)

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

    @staticmethod
    def import_parsl_config(parsl_config):
        """Import the parsl config module."""
        lsst.utils.doImport(parsl_config)

    def save_config(self, config_file):
        """Save the bps config as a pickle file."""
        with open(config_file, 'wb') as fd:
            pickle.dump(self.config, fd)

    @staticmethod
    def restore(config_file, parsl_config=None):
        """
        Restore the ParslGraph from a pickled bps config file.
        """
        # Need to have created a DimensionUniverse object to load a
        # pickled QuantumGraph.
        lsst.daf.butler.DimensionUniverse()
        with open(config_file, 'rb') as fd:
            config = pickle.load(fd)
        gwf_pickle_file = os.path.join(config['submit_path'],
                                       'bps_generic_workflow.pickle')
        with open(gwf_pickle_file, 'rb') as fd:
            generic_workflow = pickle.load(fd)
        if parsl_config is not None:
            ParslGraph.import_parsl_config(parsl_config)
        else:
            ParslGraph.import_parsl_config(config['parslConfig'])
        return ParslGraph(generic_workflow, config, do_init=False)

    def run(self, block=False):
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
        workflow.parsl_graph.run(block=True)


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
