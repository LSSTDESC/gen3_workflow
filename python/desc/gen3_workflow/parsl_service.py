"""
Parsl-based workflow management service plug-in for ctrl_bps.
"""
import os
import glob
import shutil
from collections import defaultdict
import pickle
import subprocess
import lsst.utils
import lsst.daf.butler
from lsst.daf.butler import Butler, DimensionUniverse
from lsst.ctrl.bps import BpsConfig, BPS_SEARCH_ORDER
from lsst.ctrl.bps.drivers import transform_driver
from lsst.ctrl.bps.prepare import prepare
from lsst.ctrl.bps.wms_service import BaseWmsWorkflow, BaseWmsService
from lsst.pipe.base.graph import QuantumGraph, NodeId
from desc.gen3_workflow.bash_apps import \
    small_bash_app, medium_bash_app, large_bash_app, local_bash_app
from desc.gen3_workflow.config import load_parsl_config
import parsl
from .query_workflow import query_workflow, print_status, DRP_TASKS
from .lazy_cl_handling import fix_env_var_syntax, get_input_file_paths,\
    insert_file_paths, copy_exec_butler_files


__all__ = ['start_pipeline', 'ParslGraph', 'ParslJob', 'ParslService']


_PARSL_GRAPH_CONFIG = 'parsl_graph_config.pickle'


def start_pipeline(config_file, outfile=None, mode='symlink'):
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
    outfile: str [None]
        Local link or copy of the file containing the as-run configuration
        for restarting pipelines.  The master copy is written to
        `{submitPath}/parsl_graph_config.pickle`.
    mode: str ['symlink']
        Mode for creating local copy of the file containing the
        as-run configuration.  If not 'symlink', then make a copy.

    Returns
    -------
    ParslGraph
    """
    if outfile is not None and os.path.isfile(outfile):
        raise FileExistsError(f"File exists: '{outfile}'")
    config, generic_workflow = transform_driver(config_file)
    submit_path = config['submitPath']
    workflow = prepare(config, generic_workflow, submit_path)
    as_run_config = os.path.join(submit_path, _PARSL_GRAPH_CONFIG)
    workflow.parsl_graph.dfk = load_parsl_config(config)
    workflow.parsl_graph.save_config(as_run_config)
    if outfile is not None:
        if mode == 'symlink':
            os.symlink(as_run_config, outfile)
        else:
            shutil.copy(as_run_config, outfile)
    return workflow.parsl_graph


# Job status values
_PENDING = 'pending'
_SCHEDULED = 'scheduled'
_RUNNING = 'running'
_SUCCEEDED = 'succeeded'
_FAILED = 'failed'
_EXEC_DONE = 'exec_done'

def run_command(command_line, inputs=(), stdout=None, stderr=None):
    """
    Run a command line as a parsl.bash_app.
    """
    return command_line


RUN_COMMANDS = dict(small=small_bash_app(run_command),
                    medium=medium_bash_app(run_command),
                    large=large_bash_app(run_command),
                    local=local_bash_app(run_command))

class ResourceSpecs:
    """
    Class to provide Parsl resource specifications, i.e., required
    memory, cores, and disk space.
    """
    resources = ('memory', 'cpus', 'disk')
    def __init__(self, config):
        """
        Parameters
        ----------
        config: `lsst.ctrl.bps.BpsConfig`
            Configuration of the worklow.
        """
        # Place holder set of functions for later implementation
        # that returns resource needs based on numbers of visits, etc..
        self.resource_funcs = {_: lambda *args: None for _ in self.resources}

    def resource_value(self, resource, job, *args):
        """
        Return the value for the requested resource. Note that Parsl
        measures memory in units of MBs.
        """
        func = self.resource_funcs[resource]
        if func() is None:
            # Return the resource values harvested by ctrl_bps from
            # the bps config file.
            return eval(f'job.gwf_job.request_{resource}')

        # Compute the resource need for the specific job.
        task_type = job.gwf_job.label
        return func(task_type, *args)

    def __call__(self, job, *args):
        """
        Return the parsl resource specification for the desired task.
        """
        response = dict()
        for _ in self.resources:
            value = self.resource_value(_, job, *args)
            if value is None:
                # Parsl needs a number, and using zero results in no
                # resource constraint being applied.
                value = 0
            response[_] = value
        # Parsl expects 'cores' instead of 'cpus'
        response['cores'] = response.pop('cpus')
        return response


def get_run_command(job):
    """
    Get the run command appropriate for the required resources for the
    specified job.
    """
    task_label = job.gwf_job.label

    # Get the dictionary of resource specficiations from the job.
    resource_spec = job.parent_graph.resource_specs(job)

    if 'work_queue' in job.parent_graph.dfk.executors:
        # For the workQueue, use a bash_app that passes the resource
        # specifications to parsl.
        def wq_run_command(command_line, inputs=(), stdout=None, stderr=None,
                           parsl_resource_specification=resource_spec):
            return command_line

        wq_run_command.__name__ = task_label
        wq_bash_app = parsl.bash_app(executors=['work_queue'], cache=True,
                                     ignore_for_cache=['stdout', 'stderr'])
        return wq_bash_app(wq_run_command)

    # Handle parsl configs with executors for different job_size values.
    memory_request = resource_spec['memory']/1024.   # convert to GB
    job_size = 'large'   # Default value
    try:
        for key in ('batch-small', 'batch-medium'):
            mem_per_worker = job.parent_graph.dfk.executors[key].mem_per_worker
            if mem_per_worker is None:
                # mem_per_worker is not set for this executor (and
                # presumably also not for all of the others), so just
                # use the default job_size.
                break
            if memory_request <= mem_per_worker:
                job_size = key.split('-')[1]
                break
    except AttributeError:
        # Using executors that don't have a mem_per_worker attribute.
        pass
    return RUN_COMMANDS[job_size]


@parsl.python_app(executors=['submit-node'])
def no_op_job():
    """
    A no-op parsl.python_app to return a future for a job that already
    has its outputs.
    """
    return 0


def _cmdline(gwf_job):
    """Command line for a GenericWorkflowJob."""
    return ' '.join((gwf_job.executable.src_uri, gwf_job.arguments))


class ParslJob:
    """
    Wrapper class for a GenericWorkflowJob.  This class keeps track of
    prerequisite and dependent jobs, and passes the required input
    jobs as futures to the parsl.bash_app that executes the underlying
    quantum graph.
    """
    def __init__(self, gwf_job, parsl_graph):
        """
        Parameters
        ----------
        gwf_job: `lsst.ctrl.bps.wms.GenericWorkflowJob`
            Workflow job containing execution information for the
            quantum or task to be run.
        parsl_graph: ParslGraph
            ParslGraph object that contains this ParslJob.
        """
        self.gwf_job = gwf_job
        self.config = parsl_graph.config
        self.parent_graph = parsl_graph
        self.dependencies = set()
        self.prereqs = set()
        self._done = False
        self._status = _PENDING
        self.future = None
        self._future_app = None

    def command_line(self):
        """Return the job command line to run in bash."""
        command = _cmdline(self.gwf_job) \
            + ' && >&2 echo success || (>&2 echo failure; false)'
        prefix = self.config.get('commandPrepend')
        if prefix:
            command = ' '.join([prefix, command])
        return self.parent_graph.evaluate_command_line(command, self.gwf_job)

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
        Execution state of the job based on status in monintoring.db
        or whether the job has written the 'success' string to the end
        of its log file.
        """
        if not self._done:
            if self.future is None:
                self._done = False
            elif self.parent_graph.have_monitoring_info:
                my_df = self.parent_graph.df.query(
                    f'job_name == "{self.gwf_job.name}"')
                self._done = (not my_df.empty and
                              my_df.iloc[0].status == _EXEC_DONE)
            else:
                self._done = (self.status == _SUCCEEDED)
        return self._done

    @property
    def status(self):
        """Return the job status (either _PENDING, _SCHEDULED, _RUNNING,
        _SUCCEEDED, or _FAILED) based on log file contents."""
        if self._status in (_SUCCEEDED, _FAILED):
            return self._status

        # Check log file.
        log_file = self.log_files()['stderr']
        if os.path.isfile(log_file):
            self._status = _RUNNING
            with open(log_file) as fd:
                outcome = fd.readlines()[-1]
            if outcome.startswith('success'):
                self._status = _SUCCEEDED
            elif outcome.startswith('failure'):
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

        # Define _SCHEDULED as currently _PENDING but with
        # `self.future is not None`.
        if self._status == _PENDING and self.future is not None:
            self._status = _SCHEDULED

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
            # Return a future from a no-op job, setting the function
            # name so that the monitoring db can distinguish the
            # different tasks types.
            no_op_job.__name__ = self.gwf_job.label + '_no_op'
            self.future = no_op_job()
        elif self.future is None:
            if self._future_app is not None:
                # Assuming here that self._future_apps have no prereqs.
                self.future = self._future_app()
            else:
                # Schedule the job by running the command line in the
                # appropriate parsl.bash_app.
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
        for node in self.qgraph_nodes:
            for dataset_refs in node.quantum.outputs.values():
                for dataset_ref in dataset_refs:
                    ref = registry.findDataset(dataset_ref.datasetType,
                                               dataset_ref.dataId,
                                               collections=butler.run)
                    if ref is None:
                        return False
        return True

    @property
    def qgraph_nodes(self):
        """Return the list of nodes from the underlying QuantumGraph."""
        qgraph = self.parent_graph.qgraph
        return [qgraph.getQuantumNodeByNodeId(NodeId(*_))
                for _ in [(int(self.gwf_job.cmdvals['qgraphNodeId']),
                           self.gwf_job.cmdvals['qgraphId'])]]

class StageExecButler:
    def __init__(self, exec_butler_dir, job_name):
        self.exec_butler_dir = exec_butler_dir
        self.job_name = job_name

    def __call__(self):
        return copy_exec_butler_files(self.exec_butler_dir, self.job_name)

class ParslGraph(dict):
    """
    Class to generate ParslJob objects with dependencies specified in
    the generic_worklow DAG.  This class also serves as a container
    for all of the jobs in the DAG.
    """
    def __init__(self, generic_workflow, config, do_init=True, dfk=None):
        """
        Parameters
        ----------
        generic_workflow: `lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic representation of a single workflow.
        config: `lsst.ctrl.bps.BpsConfig`
            Configuration of the worklow.
        do_init: bool [True]
            Flag to run pipetaskInit.
        dfk: parsl.DataFlowKernel [None]
            The parsl DataFlowKernel that is nominally created from
             `config['parsl_config']`.
        """
        super().__init__()
        self.gwf = generic_workflow
        self.config = config
        self.resource_specs = ResourceSpecs(self.config)
        if do_init:
            self._pipetaskInit()
        self.dfk = dfk
        self._ingest()
        self._qgraph = None
        self.monitoring_db = './monitoring.db'
        self.have_monitoring_info = False
        try:
            self._update_status()
        except FileNotFoundError:
            self._update_status_from_logs()

    def _ingest(self):
        """Ingest the workflow as ParslJobs."""
        for job_name in self.gwf:
            if job_name == 'pipetaskInit':
                continue

            # Make sure pipelines without downstream dependencies are
            # ingested into the ParslGraph.
            _ = self[job_name]

            if self.config['executionButlerDir']:
                # Add a jobs to stage execution butler files
                file_staging_name = job_name + '_stage_exec_butler'
                self[file_staging_name].add_dependency(self[job_name])
                self[job_name].add_prereq(self[file_staging_name])
                self[file_staging_name]._future_app = parsl.python_app(
                    StageExecButler(self.config['executionButlerDir'], job_name))

            # Collect downstream dependencies and prerequisites for
            # each job.
            for successor_job in self.gwf.successors(job_name):
                self[job_name].add_dependency(self[successor_job])
                self[successor_job].add_prereq(self[job_name])

    def _update_status(self):
        """
        Update the pandas dataframe containing the workflow status using
        the monitoring db.
        """
        import pandas as pd
        # Get job status values from monitoring db.
        df = query_workflow(self.config['outCollection'],
                            db_file=self.monitoring_db)
        # Make entries for jobs that are not yet in the monitoring db.
        current_jobs = set() if df.empty else set(df['job_name'])
        data = defaultdict(list)
        for job_name in self:
            if job_name in current_jobs or job_name.endswith('stage_exec_butler'):
                continue
            data['job_name'].append(job_name)
            data['task_type'].append(job_name.split('_')[1])
            data['status'].append(_PENDING)
        self.df = pd.concat((df, pd.DataFrame(data=data)))
        self.task_types = set(self.df['task_type'])
        self.have_monitoring_info = True

    def _update_status_from_logs(self):
        """
        Update the pandas dataframe containing the workflow status and
        job metadata using the task log files.
        """
        import pandas as pd
        self.task_types = []
        data = defaultdict(list)
        _, template_id = self.config.search('templateDataId',
                                            opt=dict(replaceVars=False))
        md_columns = [_.strip('{}') for _ in template_id.split('_')]
        md_columns.insert(0, 'task_type')
        def int_cast(value):
            try:
                return int(value)
            except ValueError:
                return value
        for job_name, job in self.items():
            if job_name.endswith('_stage_exec_butler'):
                continue
            md = {_: '' for _ in md_columns}
            for key, value in zip(md_columns, job_name.split('_')[1:]):
                md[key] = int_cast(value)
            for key, value in md.items():
                data[key].append(value)
            if data['task_type'][-1] not in self.task_types:
                self.task_types.append(data['task_type'][-1])
            data['job_name'].append(job_name)
            data['status'].append(job.status)
        self.df = pd.DataFrame(data=data)

    @property
    def qgraph(self):
        """The QuantumGraph associated with the current bps job."""
        if self._qgraph is None:
            qgraph_file = glob.glob(os.path.join(self.config['submitPath'],
                                                 '*.qgraph'))[0]
            self._qgraph = QuantumGraph.loadUri(qgraph_file, DimensionUniverse())
        return self._qgraph

    def get_jobs(self, task_type, status='pending', query=None):
        """
        Return a list of job names for the specified task applying an
        optional query on the status data frame.
        """
        my_query = f'(task_type == "{task_type}")'
        if status is not None:
            my_query += f' and status == "{status}"'
        if query is not None:
            my_query = ' and '.join((my_query, query))
        return list(self.df.query(my_query)['job_name'])

    def status(self, use_logs=False):
        """Print a summary of the workflow status."""
        if not use_logs:
            try:
                self._update_status()
                task_types = [_ for _ in DRP_TASKS if _ in self.task_types]
                for item in self.task_types:
                    if item not in task_types:
                        task_types.append(item)
                print_status(self.df, task_types)
                return
            except FileNotFoundError:
                pass
        self._update_status_from_logs()
        summary = ['task type                '
                   'pending  scheduled  running  succeeded  failed  total\n']
        for task_type in self.task_types:
            my_df = self.df.query(f'task_type == "{task_type}"')
            num_tasks = len(my_df)
            num_pending = len(my_df.query(f'status == "{_PENDING}"'))
            num_scheduled = len(my_df.query(f'status == "{_SCHEDULED}"'))
            num_running = len(my_df.query(f'status == "{_RUNNING}"'))
            num_succeeded = len(my_df.query(f'status == "{_SUCCEEDED}"'))
            num_failed = len(my_df.query(f'status == "{_FAILED}"'))
            summary.append(f'{task_type:25s}  {num_pending:5d}      '
                           f'{num_scheduled:5d}    '
                           f'{num_running:5d}      {num_succeeded:5d}   '
                           f'{num_failed:5d}  {num_tasks:5d}')
        print('\n'.join(summary))

    def __getitem__(self, job_name):
        """
        Re-implementation of dict.__getitem__ to create ParslJobs
        from each of the named jobs in the generic_workflow.
        """
        if not job_name in self:
            try:
                gwf_job = self.gwf.get_job(job_name)
            except KeyError:
                gwf_job = None
            super().__setitem__(job_name, ParslJob(gwf_job, self))
        return super().__getitem__(job_name)

    def evaluate_command_line(self, command, gwf_job):
        """
        Evaluate command line, replacing bps variables, fixing env vars,
        and inserting job-specific file paths, all assuming that
        everything is running on a shared file system.
        """
        command = command.format(**gwf_job.cmdvals)
        command = fix_env_var_syntax(command)
        file_paths = get_input_file_paths(self.gwf, gwf_job.name)
        return insert_file_paths(command, file_paths)

    def _pipetaskInit(self):
        """If the output collection isn't in the repo, run pipetaskInit."""
        butler = Butler(self.config['butlerConfig'])
        job_name = 'pipetaskInit'
        if self.config['outCollection'] not in \
           butler.registry.queryCollections():
            pipetaskInit = self.gwf.get_job(job_name)
            command = 'time ' + _cmdline(pipetaskInit)
            # pipetaskInit needs to handle the butlerConfig differently
            # than QG jobs since the execution butler doesn't yet exist.
            command = command.replace('<FILE:butlerConfig>',
                                      self.config['butlerConfig'])
            command = self.evaluate_command_line(command, pipetaskInit)
            subprocess.check_call(command, shell=True)

    @staticmethod
    def import_parsl_config(parsl_config):
        """Import the parsl config module."""
        return lsst.utils.doImport(parsl_config)

    def save_config(self, config_file):
        """Save the bps config as a pickle file."""
        with open(config_file, 'wb') as fd:
            pickle.dump(self.config, fd)

    @staticmethod
    def restore(config_file, parsl_config=None):
        """
        Restore the ParslGraph from a pickled bps config file.

        Parameters
        ----------
        config_file: str
            The filename of the pickle file containing the
            as-run config from the ParslGraph object.
        parsl_config: str [None]
            A parslConfig to supply an alternative configuration
            for the DataFlowKernel instead of the one in the original
            bps config yaml file.  For example,
            'desc.gen3_workflow.config.thread_pool_config_4'
            could be provided to run interactively using the local node's
            resources.

        Returns
        -------
        ParslGraph object
        """
        from lsst.ctrl.bps import BpsConfig, BPS_SEARCH_ORDER
        # Need to have created a DimensionUniverse object to load a
        # pickled QuantumGraph.
        lsst.daf.butler.DimensionUniverse()
        with open(config_file, 'rb') as fd:
            config = pickle.load(fd)
        gwf_pickle_file = os.path.join(config['submitPath'],
                                       'bps_generic_workflow.pickle')
        with open(gwf_pickle_file, 'rb') as fd:
            generic_workflow = pickle.load(fd)

        if parsl_config is not None:
            if os.path.isfile(parsl_config):
                my_config = BpsConfig(parsl_config, BPS_SEARCH_ORDER)
                config['parsl_config'] = my_config['parsl_config']
            else:
                config['parslConfig'] = parsl_config

        dfk = load_parsl_config(config)

        return ParslGraph(generic_workflow, config, do_init=False, dfk=dfk)

    def run(self, jobs=None, block=False, finalize=True):
        """
        Run the encapsulated workflow by requesting the futures of
        the requested jobs or of those at the endpoints of the DAG.
        """
        if jobs is not None:
            futures = [self[job_name].get_future() for job_name in jobs]
        else:
            # Run all of the jobs at the endpoints of the DAG
            futures = [job.get_future() for job in self.values()
                       if not job.dependencies]

        if block:
            # Calling .result() for each future blocks returning from
            # this method until all the jobs have executed.  This is
            # needed for running in a non-interactive python process
            # that would otherwise end before the futures resolve.
            _ = [future.result() for future in futures]
            # Since we're running non-interactively, run self.finalize()
            # to transfer datasets to the destination butler.
            if finalize:
                self.finalize()

    def finalize(self):
        """Run final job to transfer datasets from the execution butler to
        the destination repo butler."""
        command = (f"bash {self.config['submitPath']}/final_job.bash "
                   f"{self.config['butlerConfig']} "
                   f"{self.config['executionButlerTemplate']}")
        subprocess.check_call(command, shell=True)

    def clean_up_exec_butler_files(self):
        """Clean up the copies of the execution butler."""
        temp_repo_dir = os.path.join(self.config['executionButlerTemplate'],
                                     'tmp_repos')
        if os.path.isdir(temp_repo_dir):
            shutil.rmtree(temp_repo_dir)


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
        service_class = 'desc.gen3_workflow.ParslService'
        workflow = ParslWorkflow.\
            from_generic_workflow(config, generic_workflow, out_prefix,
                                  service_class)

        return workflow

    def submit(self, workflow):
        """
        Submit a workflow for execution.

        Parameters
        ----------
        workflow: `desc.gen3_workflow.ParslWorkflow`
            Workflow object to execute.
        """
        # Import the parsl config and set the DataFlowKernel attribute.
        workflow.parsl_graph.dfk \
            = load_parsl_config(workflow.parsl_graph.config)

        workflow.parsl_graph.run(block=True)

    def cancel(self, wms_id, pass_thru=None):
        """Not implemented"""
        raise NotImplementedError

    def list_submitted_jobs(self, wms_id=None, user=None, require_bps=True,
                            pass_thru=None):
        """Not implemented"""
        raise NotImplementedError

    def report(self, wms_workflow_id=None, user=None, hist=0, pass_thru=None):
        """Not implemented"""
        raise NotImplementedError


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
        parsl_graph_config = os.path.join(out_prefix, _PARSL_GRAPH_CONFIG)
        parsl_workflow.parsl_graph.save_config(parsl_graph_config)
        return parsl_workflow

    def write(self, out_prefix):
        """Not implemented"""
        raise NotImplementedError
