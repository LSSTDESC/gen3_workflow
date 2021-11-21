"""
Code to count numbers of input datasets of specified type that
goes into the output data products for a given pipeline task.
"""
import glob
from collections import defaultdict
from lsst.daf.butler import DimensionUniverse
from lsst.pipe.base.graph import QuantumGraph
import pandas as pd


__all__ = ['count_task_inputs']


def count_task_inputs(qgraph_file, task_label='assembleCoadd',
                      input_type='deepCoadd_directWarp'):
    """
    Gather information on the numbers of input datasets of the
    specified dataset type going into the output data products of the
    specified task as determined from the QuantumGraph in the
    qgraph_file.  Also, count the number of instances per task type in
    the QuantumGraph.

    Parameters
    ----------
    qgraph_file: str
        Filename of the QuantumGraph file to consider.
    task_label: str ['assembleCoadd']
        Label of the task from the pipeline yaml file.  Appropriate
        labels include 'assembleCoadd', 'templateGen', 'makeWarp'.
    input_type: str ['deepCoadd_directWarp']
        Dataset type name for the inputs to consider.  Appropriate
        dataset types include 'deepCoadd_directWarp', 'calexp'.

    Returns
    -------
    (pandas.DataFrame with rows indexed by the task dataId,
    dict containing the numbers of instance for each task type)
    """
    qgraph = QuantumGraph.loadUri(qgraph_file, DimensionUniverse())

    data = defaultdict(list)
    task_counts = defaultdict(lambda: 0)
    for i, node in enumerate(qgraph):
        task_counts[node.taskDef.label] += 1
        if node.taskDef.label == task_label:
            dataId = node.quantum.dataId
            for dim in dataId:
                data[dim].append(dataId[dim])
            for dstype, dsrefs in node.quantum.inputs.items():
                if dstype.name == input_type:
                    data[f'num_{input_type}'].append(len(dsrefs))
                    break
    return pd.DataFrame(data=data), task_counts
