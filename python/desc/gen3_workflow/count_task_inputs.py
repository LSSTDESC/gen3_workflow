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
                      input_name='deepCoadd_directWarp'):
    """
    Return a dataframe with the numbers input datasets of specified
    dataset types going into the output data products of the specified
    task as determined from the QuantumGraph in the qgraph_file.

    Parameters
    ----------
    qgraph_file: str
        Filename of the QuantumGraph file to consider.
    task_label: str ['assembleCoadd']
        Label of the task from the pipeline yaml file.  Appropriate
        labels include 'assembleCoadd', 'templateGen', 'makeWarp'.
    input_name: str ['deepCoadd_directWarp']
        Dataset type name for the inputs to consider.  Appropriate
        dataset types include 'deepCoadd_directWarp', 'calexp'.

    Returns
    -------
    pandas.DataFrame with rows indexed by the task dataId.
    """
    qgraph = QuantumGraph.loadUri(qgraph_file, DimensionUniverse())

    data = defaultdict(list)
    for i, node in enumerate(qgraph):
        if node.taskDef.label == task_label:
            dataId = node.quantum.dataId
            for dim in dataId:
                data[dim].append(dataId[dim])
            for dstype, dsrefs in node.quantum.inputs.items():
                if dstype.name == input_name:
                    data[f'num_{input_name}'] = len(dsrefs)
                    break
    return pd.DataFrame(data=data)
