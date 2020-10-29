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
import numpy as np
import networkx
from lsst.daf.butler import DimensionUniverse, Butler
from lsst.pipe.base import QuantumGraph

import parsl
from parsl import bash_app
try:
    parsl.load()
except RuntimeError:
    pass


@bash_app
def run_quantum(task, inputs=()):
    configs = dict(task.task_graph.config)
    configs['quantum_file'] = task.write_subgraph()
    command = '''time pipetask run -b %(butlerConfig)s \\
        -i %(inCollection)s \\
        --output-run %(outCollection)s --extend-run --skip-init-writes \\
        --qgraph %(quantum_file)s --skip-existing \\
        --no-versions''' % configs
    return command


FILENODE = 0
TASKNODE = 1


class ScienceGraph(networkx.DiGraph):
    """
    Subclass of networkx.DiGraph that contains the DAG representation
    of a QuantumGraph and keeps a dictionary of the individual quantum
    nodes for execution using the `pipetask` command.
    """
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
        with open(qgraph_file, 'rb') as fd:
            self.qgraph = QuantumGraph.load(fd, DimensionUniverse())
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
            tnode_name = "task%d (%s)" % (ncnt, task_def.taskName)
            self.add_node(tnode_name, node_type=TASKNODE,
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
                    self.add_node(fnode_name, node_type=FILENODE)
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
                    self.add_node(fnode_name, node_type=FILENODE)
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
        self.done = False
        self.task_graph = task_graph
        self.qgnode = task_graph.sci_graph.qgnodes[taskname]
        self._future = None

    def add_dependency(self, dependency):
        """Add a Task dependent on the current one."""
        self.dependencies.add(dependency)

    def add_prereq(self, prereq):
        """
        Add a prequisite task, setting its state as False, indicating that
        the prerequisite task's outputs have not been generated.
        """
        self.prereqs[prereq] = False

    def run(self):
        """
        Run the quantum node in this Task using `pipetask`.
        """
        if not all(self.prereqs.values()) or self.done:
            return
        print(f'running "{self.taskname}":')
        configs = dict(self.task_graph.config)
        configs['quantum_file'] = self.write_subgraph()
        command = '''time pipetask run -b %(butlerConfig)s \\
        -i %(inCollection)s \\
        --output-run %(outCollection)s --extend-run --skip-init-writes \\
        --qgraph %(quantum_file)s --skip-existing \\
        --no-versions''' % configs
        print(command)
        sys.stdout.flush()
        subprocess.check_call(command, shell=True)
        print('\n')
        self.finish()

    def write_subgraph(self):
        """
        Write a subgraph containing the quantum node to a pickle file
        so that that file can be provided as an argument to `pipetask`
        for execution.
        """
        qgnode_dir = self.task_graph.config['qgnode_dir']
        task_id = self.taskname.split()[0]
        quantum_file = os.path.join(qgnode_dir, f'{task_id}.pickle')
        subgraph = self.task_graph.sci_graph.qgraph.subset(self.qgnode)
        os.makedirs(os.path.dirname(quantum_file), exist_ok=True)
        with open(quantum_file, 'wb') as fd:
            subgraph.save(fd)
        return quantum_file

    def finish(self):
        """
        Finish this task by setting the `done` attribute to `True`
        and updating its dependent task that its outputs have been
        generated.
        """
        self.done = True
        for dependency in self.dependencies:
            if self not in dependency.prereqs:
                raise RuntimeError('inconsistent dependency')
            dependency.prereqs[self] = True

    def __str__(self):
        return self.taskname

    def __repr__(self):
        return f'Task("{self.taskname}")'

    @property
    def future(self):
        if self._future is None:
            inputs = [_.future for _ in self.prereqs]
            self._future = run_quantum(self, inputs=inputs)
        return self._future


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
            if self._have_outputs(task.qgnode.quantum):
                task.finish()

    @property
    def tasks(self):
        """
        A list of the Task objects contained in this TaskGraph.  This list
        is randomized initially to mimic asynchronous execution of the
        pipeline components.
        """
        if self._tasks is None:
            self._tasks = list(super().values())
            #np.random.shuffle(self._tasks)
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
        for datasetRefs in quantum.outputs.values():
            for datasetRef in datasetRefs:
                ref = registry.findDataset(datasetRef.datasetType,
                                           datasetRef.dataId,
                                           collections=butler.run)
                if ref is None:
                    return False
        return True

    def run_pipeline(self):
        """Run the pipeline tasks sequentially until they are all done."""
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
            task_type = task.taskname.split()[1].strip('()')
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
    config = dict(cp.items('DEFAULT'))

    task_graph = TaskGraph(config)

    if args.summary:
        task_graph.summary()

    if args.run:
        task_graph.run_pipeline()
