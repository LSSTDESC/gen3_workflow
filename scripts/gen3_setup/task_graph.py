"""
Code to generate a DAG representation of a QuantumGraph and
provide a simple execution framework for those task while keeping
track of which tasks have generated all of their output products.
"""
import os
import sys
import itertools
import subprocess
from collections import defaultdict
import configparser
import networkx
import parsl
from lsst.daf.butler import DimensionUniverse, Butler
from lsst.pipe.base import QuantumGraph


@parsl.bash_app
def run_quantum(task, inputs=(), stdout=None, stderr=None):
    """
    Run the quantum associated with the specified task.
    If the task's outputs are present in the repo, it's done so
    return an empty string as a no-op command line.
    """
    if task.done:
        return ''
    return task.command_line()


class ScienceGraph(networkx.DiGraph):
    """
    Subclass of networkx.DiGraph that contains the DAG representation
    of a QuantumGraph and keeps a dictionary of the individual quantum
    nodes for execution using the `pipetask` command.
    """
    FILENODE = 0
    TASKNODE = 1
    def __init__(self, qgraph_file):
        """
        Parameters
        ----------

        qgraph_file: Pickle file containing the QuantumGraph
            representation of the pipeline to run.
        """
        super().__init__()
        self.qgnodes = None
        self.task_list = None
        with open(qgraph_file, 'rb') as input_:
            self.qgraph = QuantumGraph.load(input_, DimensionUniverse())
        self._read_quantum_graph()

    def _read_quantum_graph(self):
        """
        This code is adapted from
        https://github.com/lsst/ctrl_bps/blob/master/python/lsst/ctrl/bps/bps_core.py
        and modified for this lighter weight class.

        Create expanded graph from the QuantumGraph that has explicit
        dependencies and has individual nodes for each input/output
        dataset.
        """
        ncnt = 0
        dcnt = 0
        dsname_to_node_id = {}
        self.qgnodes = {}
        task_list = []
        for task_id, node in enumerate(self.qgraph):
            task_def = node.taskDef
            if task_def.taskName not in task_list:
                task_list.append(task_def.taskName)
            ncnt += 1
            tnode_name = f"task_{ncnt:06d}_{task_def.taskName}"
            self.add_node(tnode_name, node_type=self.TASKNODE,
                          task_def_id=task_id, task_abbrev=task_def.label)
            self.qgnodes[tnode_name] = node
            inputs, outputs = node.quantum.inputs, node.quantum.outputs
            # Make dataset ref nodes for inputs
            for ds_ref in itertools.chain.from_iterable(inputs.values()):
                ds_name = f"{ds_ref.datasetType.name}+{ds_ref.dataId}"
                if ds_name not in dsname_to_node_id:
                    ncnt += 1
                    dcnt += 1
                    fnode_name = f"ds{dcnt:06d}"
                    dsname_to_node_id[ds_name] = fnode_name
                    self.add_node(fnode_name, node_type=self.FILENODE)
                fnode_name = dsname_to_node_id[ds_name]
                self.add_edge(fnode_name, tnode_name)

            # Make dataset ref nodes for outputs
            for ds_ref in itertools.chain.from_iterable(outputs.values()):
                ds_name = f"{ds_ref.datasetType.name}+{ds_ref.dataId}"
                if ds_name not in dsname_to_node_id:
                    ncnt += 1
                    dcnt += 1
                    fnode_name = f"ds{dcnt:06d}"
                    dsname_to_node_id[ds_name] = fnode_name
                    self.add_node(fnode_name, node_type=self.FILENODE)
                fnode_name = dsname_to_node_id[ds_name]
                self.add_edge(tnode_name, fnode_name)
        self.task_list = task_list


class Task:
    """
    A class to contain a quantum node and to keep track of
    prerequisite and dependent tasks in the pipeline.
    """
    def __init__(self, taskname, task_graph):
        """
        Parameters
        ----------
        taskname: str
            Name of the task.
        task_graph: TaskGraph
            Reference to the TaskGraph object of which this Task
            instance is a part.
        """
        self.taskname = taskname
        self.dependencies = set()
        self.prereqs = dict()
        self.task_graph = task_graph
        self.qgnode = task_graph.sci_graph.qgnodes[taskname]
        self._done = False
        self.future = None

    def add_dependency(self, dependency):
        """Add a Task dependent on the current one."""
        self.dependencies.add(dependency)

    def add_prereq(self, prereq):
        """
        Add a prequisite task, setting its state as False, indicating that
        the prerequisite task's outputs have not been generated.
        """
        self.prereqs[prereq] = False

    def command_line(self):
        """
        Write the QuantumGraph node associated with this task as a
        subgraph to a pickle file and generate the `pipetask run` command
        line to execute the subgraph.
        """
        config = dict(self.task_graph.config)
        config['quantum_file'] = self.write_subgraph()
        return '''time pipetask run -b %(butlerConfig)s \\
        -i %(inCollection)s \\
        --output-run %(outCollection)s --extend-run --skip-init-writes \\
        --qgraph %(quantum_file)s --skip-existing \\
        --no-versions''' % config

    def run(self):
        """
        Run the quantum node in this Task using `pipetask`.
        """
        if self.done:
            return
        command = self.command_line()
        print(command)
        self.get_future().result()
        print('\n')
        self.finish()

    def write_subgraph(self):
        """
        Write a subgraph containing the quantum node to a pickle file
        so that that file can be provided as an argument to `pipetask`
        for execution.
        """
        qgnode_dir = self.task_graph.config['qgnode_dir']
        quantum_file = os.path.join(qgnode_dir, f'{self.taskname}.pickle')
        subgraph = self.task_graph.sci_graph.qgraph.subset(self.qgnode)
        os.makedirs(os.path.dirname(quantum_file), exist_ok=True)
        with open(quantum_file, 'wb') as output:
            subgraph.save(output)
        return quantum_file

    def finish(self):
        """
        Finish this task by setting the `done` attribute to `True`
        and updating its dependent task that its outputs have been
        generated.
        """
        self._done = True
        for dependency in self.dependencies:
            if self not in dependency.prereqs:
                raise RuntimeError('inconsistent dependency')
            dependency.prereqs[self] = True

    def __str__(self):
        return self.taskname

    def __repr__(self):
        return f'Task("{self.taskname}")'

    @property
    def done(self):
        """Return True if this task has been executed."""
        if (self.future is not None and self.future.done()
            and self.future.exception() is None):
            self.finish()
        return self._done

    def log_files(self):
        """
        Return a dict of filenames for directing stderr and stdout.
        """
        log_dir = self.task_graph.logging_dir
#        return dict(stderr=os.path.join(log_dir, f'{self.taskname}.stderr'),
#                    stdout=os.path.join(log_dir, f'{self.taskname}.stdout'))
        return dict(stderr=os.path.join(log_dir, f'{self.taskname}.stderr'))

    def get_future(self):
        """
        Return the future of the run_quantum bash_app that is
        used to execute this task, collecting input futures from
        this task's prerequisite tasks.
        """
        if self.future is None:
            inputs = [_.get_future() for _ in self.prereqs]
            self.future = run_quantum(self, inputs=inputs, **self.log_files())
        return self.future


class TaskGraph(dict):
    """
    Class to contain the DAG of the pipeline and to manage the
    execution of the tasks.
    """
    def __init__(self, config):
        """
        Parameters
        ----------
        config: dict
            Dictionary of the config parameters.
        """
        super().__init__()
        self.config = config
        self.butler = Butler(config['butlerConfig'])
        self._tasks = None
        self.ingest_pipeline()
        self.logging_dir = os.path.abspath(config['logging_dir'])
        os.makedirs(self.logging_dir, exist_ok=True)

    def ingest_pipeline(self):
        """
        Ingest the QuantumGraph as a DAG and set up the prerequisites
        and dependencies, and initialize the output collection in the
        data repo.
        """
        self.sci_graph = ScienceGraph(self.config['qgraph_file'])
        for task_name in self.sci_graph:
            if not task_name.startswith('task'):
                continue
            for output in self.sci_graph.successors(task_name):
                for successor_task in self.sci_graph.successors(output):
                    self[task_name].add_dependency(self[successor_task])
                    self[successor_task].add_prereq(self[task_name])
        if self.config['outCollection'] not in \
           self.butler.registry.queryCollections():
            self._init_out_collection()
        self._set_state()

    def __getitem__(self, key):
        if not key in self:
            super().__setitem__(key, Task(key, self))
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        if key in self:
            raise RuntimeError(f'{key} already in TaskGraph')
        super().__setitem__(key, value)
        self[key].task_graph = self

    def _set_state(self):
        for task in self.tasks:
            if ((task.future is not None and task.future.done)
                or self._have_outputs(task.qgnode.quantum)):
                task.finish()

    @property
    def tasks(self):
        """
        A list of the Task objects contained in this TaskGraph.
        """
        if self._tasks is None:
            self._tasks = list(super().values())
        return self._tasks

    def _have_outputs(self, quantum):
        """
        Return True if all of the output datasets from this quantum
        have been generated, otherwise return False.
        """
        # Using self.butler here fails if this function is called
        # right after self._init_out_collection is called, but making
        # a new Butler object with the run option set seems to work
        # consistently.
        butler = Butler(self.config['butlerConfig'],
                        run=self.config['outCollection'])
        registry = butler.registry
        for dataset_refs in quantum.outputs.values():
            for dataset_ref in dataset_refs:
                ref = registry.findDataset(dataset_ref.datasetType,
                                           dataset_ref.dataId,
                                           collections=butler.run)
                if ref is None:
                    return False
        return True

    def get_tasks_by_name(self, taskname, remaining=True):
        """
        Return all tasks by partial name, returning the ones not
        done by default.
        """
        my_tasks = []
        for task in self.tasks:
            if taskname in task.taskname and (not remaining or not task.done):
                my_tasks.append(task)
        return my_tasks

    def reset_exceptions(self):
        """Reset all tasks that finished with an exception."""
        for task in self.tasks:
            if task.future is not None and task.future.exception() is not None:
                task.future = None
                task._done = False

    def run_pipeline(self):
        """
        Run the pipeline tasks sequentially until they are all done.  This
        function will not in general take advantage of possible
        concurrency in running the tasks. Greater concurrency can be
        achieved by calling .get_future() for the most downstream tasks
        and letting parsl execute the futures of prerequisite tasks.
        """
        while not self.done():
            for task in self.tasks:
                task.run()

    def _init_out_collection(self):
        config = self.config
        command = '''time pipetask run -b %(butlerConfig)s \\
        -i %(inCollection)s \\
        --output-run %(outCollection)s --init-only --skip-existing \\
        --register-dataset-types --qgraph %(qgraph_file)s \\
        --no-versions''' % config
        print(f"\nInitializing output collection {config['outCollection']}\n"
              f"for {config['qgraph_file']}:\n")
        print(command)
        sys.stdout.flush()
        subprocess.check_call(command, shell=True)
        print()

    def done(self):
        """
        Return True if all tasks have finished.
        """
        return all(_.done for _ in self.values())

    def state(self):
        """
        Return a string describing the current state of the pipeline.
        """
        items = []
        for task_name, task in self.items():
            items.append(task_name)
            items.append('   prereqs: ' + str(task.prereqs))
            items.append('   dependencies: ' + str(task.dependencies))
            items.append('')
        return '\n'.join(items)

    def summary(self):
        """
        Print a summary of the current state of the pipeline.
        """
        counts = defaultdict(list)
        for task in self.tasks:
            task_type = task.taskname.split('_')[-1]
            counts[task_type].append(1 if task.done else 0)
        for task_type in self.sci_graph.task_list:
            values = counts[task_type]
            print(task_type, sum(values), len(values))
        print()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('config_file',
                        help='Config file containing pipeline information.')
    parser.add_argument('--summary', default=False, action='store_true',
                        help='Print a summary of the pipeline state.')
    parser.add_argument('--run', default=False, action='store_true',
                        help=('Run the pipeline, starting from the '
                              'current state.'))
    args = parser.parse_args()

    cp = configparser.ConfigParser()
    cp.optionxform = str
    cp.read(args.config_file)
    configs = dict(cp.items('DEFAULT'))

    my_task_graph = TaskGraph(configs)

    if args.summary:
        my_task_graph.summary()

    if args.run:
        my_task_graph.run_pipeline()
