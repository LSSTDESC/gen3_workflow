import os
import sys
import re
import itertools
import subprocess
from collections import defaultdict
import configparser
import numpy as np
import networkx
from lsst.daf.butler import DimensionUniverse, Butler
from lsst.pipe.base import QuantumGraph

FILENODE = 0
TASKNODE = 1


class PipelineGraph:
    def __init__(self, qgraph_file):
        self.sci_graph = None
        self.qgnodes = None
        self.task_list = None
        with open(qgraph_file, 'rb') as fd:
            self.qgraph = QuantumGraph.load(fd, DimensionUniverse())
        self.create_science_graph()

    def create_science_graph(self):
        """
        This code is copied from
        https://github.com/lsst/ctrl_bps/blob/master/python/lsst/ctrl/bps/bps_core.py
        and modified for this lighter weight class.

        Create expanded graph from the QuantumGraph that has explicit
        dependencies and has individual nodes for each input/output
        dataset.
        """
        self.sci_graph = networkx.DiGraph()
        ncnt = 0
        dcnt = 0
        dsname_to_node_id = {}
        self.qgnodes = {}
        task_list = []
        for task_id, node in enumerate(self.qgraph):
            task_def = node.taskDef
            task_list.append(task_def.taskName)
            ncnt += 1
            tnode_name = "task%d (%s)" % (ncnt, task_def.taskName)
            self.sci_graph.add_node(tnode_name, node_type=TASKNODE,
                                    task_def_id=task_id,
                                    task_abbrev=task_def.label)
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
                    self.sci_graph.add_node(fnode_name, node_type=FILENODE)
                fnode_name = dsname_to_node_id[ds_name]
                self.sci_graph.add_edge(fnode_name, tnode_name)

            # Make dataset ref nodes for outputs
            for ds_ref in itertools.chain.from_iterable(outputs.values()):
                ds_name = f"{ds_ref.datasetType.name}+{ds_ref.dataId}"
                if ds_name not in dsname_to_node_id:
                    ncnt += 1
                    dcnt += 1
                    fnode_name = f"ds{dcnt:06d}"
                    dsname_to_node_id[ds_name] = fnode_name
                    self.sci_graph.add_node(fnode_name, node_type=FILENODE)
                fnode_name = dsname_to_node_id[ds_name]
                self.sci_graph.add_edge(tnode_name, fnode_name)
        self.task_list = task_list


class Task:
    def __init__(self, taskname, task_graph):
        self.taskname = taskname
        self.dependencies = set()
        self.prereqs = dict()
        self.done = False
        self.task_graph = task_graph
        self.qgnode = task_graph.pipeline.qgnodes[taskname]

    def add_dependency(self, dependency):
        self.dependencies.add(dependency)

    def add_prereq(self, prereq):
        self.prereqs[prereq] = False

    def run(self):
        qgnode = self.qgnode
        qgnode_dir = self.task_graph.config['qgnode_dir']
        if not all(self.prereqs.values()) or self.done:
            return
        print('running', self.taskname)
        task_id = self.taskname.split()[0]
        quantum_file = os.path.join(qgnode_dir, f'{task_id}.pickle')
        self.write_subgraph(qgnode, quantum_file)
        configs = dict(self.task_graph.config)
        configs['quantum_file'] = quantum_file
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

    def write_subgraph(self, qgnodes, outfile):
        qgraph = self.task_graph.pipeline.qgraph
        subgraph = qgraph.subset(qgnodes)
        os.makedirs(os.path.dirname(outfile), exist_ok=True)
        with open(outfile, 'wb') as fd:
            subgraph.save(fd)

    def finish(self):
        self.done = True
        for dependency in self.dependencies:
            if self not in dependency.prereqs:
                raise RuntimeError('inconsistent dependency')
            dependency.prereqs[self] = True

    def __str__(self):
        return self.taskname

    def __repr__(self):
        return f'Task("{self.taskname}")'


class TaskGraph(dict):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.butler = Butler(config['butlerConfig'])
        self._tasks = None
        self.ingest_pipeline()

    def ingest_pipeline(self):
        self.pipeline = PipelineGraph(self.config['qgraph_file'])
        graph = self.pipeline.sci_graph
        for task_name in graph:
            if not task_name.startswith('task'):
                continue
            for output in graph.successors(task_name):
                for successor_task in graph.successors(output):
                    self[task_name].add_dependency(self[successor_task])
                    self[successor_task].add_prereq(self[task_name])
        if self.config['outCollection'] not in \
           self.butler.registry.queryCollections():
            self._init_out_collection()
        if self.config['outCollection'] in \
           self.butler.registry.queryCollections():
            self._set_state()

    def __getitem__(self, key):
        if not key in self:
            super().__setitem__(key, Task(key, self))
            self[key].task_graph = self
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        if key in self:
            raise RuntimeError(f'{key} already in TaskGraph')
        super().__setitem__(key, value)
        self[key].task_graph = self

    def _set_state(self):
        for task in self.tasks:
            if self._have_quantum_outputs(task.qgnode.quantum):
                task.finish()

    @property
    def tasks(self):
        if self._tasks is None:
            self._tasks = list(super().values())
            np.random.shuffle(self._tasks)
        return self._tasks

    def _have_quantum_outputs(self, quantum):
        collections = [self.config['outCollection']]
        registry = self.butler.registry
        for datasetRefs in quantum.outputs.values():
            for datasetRef in datasetRefs:
                ref = registry.findDataset(datasetRef.datasetType,
                                           datasetRef.dataId,
                                           collections=collections)
                if ref is None:
                    return False
        return True

    def run_pipeline(self):
        while not self.done():
            for task in self.tasks:
                task.run()

    def _init_out_collection(self):
        command = '''time pipetask run -b %(butlerConfig)s \\
            -i %(inCollection)s \\
            --output-run %(outCollection)s --init-only --skip-existing \\
            --register-dataset-types --qgraph %(qgraph_file)s \\
            --no-versions''' % self.config
        print(command)
        sys.stdout.flush()
        try:
            subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError as eobj:
            print("Error encountered initializing the pipeline:")
            print(eobj)
        print('\n')

    def done(self):
        return all(_.done for _ in self.values())

    def state(self):
        items = []
        for task_name, task in self.items():
            items.append(task_name)
            items.append('   prereqs: ' + str(task.prereqs))
            items.append('   dependencies: ' + str(task.dependencies))
            items.append('')
        return '\n'.join(items)

    def summary(self):
        counts = defaultdict(list)
        for task in self.tasks:
            task_type = task.taskname.split()[1].strip('()')
            counts[task_type].append(1 if task.done else 0)
        for task_type in self.pipeline.task_list:
            values = counts[task_type]
            print(task_type, sum(values), len(values))
        print()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f'usage: python {sys.argv[0]} <config_file>')
        sys.exit(0)

    cp = configparser.ConfigParser()
    cp.optionxform = str
    cp.read(sys.argv[1])
    config = dict(cp.items('DEFAULT'))

    task_graph = TaskGraph(config)

    task_graph.summary()

    task_graph.run_pipeline()

    task_graph.summary()
