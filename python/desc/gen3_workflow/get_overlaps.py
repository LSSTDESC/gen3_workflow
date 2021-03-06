"""
Module for finding calexps that overlap tracts and patches from
the GenericWorkflowGraph via a ParslGraph.
"""
from collections import defaultdict
import pandas as pd

__all__ == ['get_overlaps']

def get_overlaps(graph):
    """
    Find the calexps contributing to a given band-tract-patch.

    Parameters
    ----------
    graph: ParslGraph

    Returns
    -------
    pandas.DataFrame
    """
    warp_job_names \
        = list(graph._status_df.query('task_type=="makeWarp"')['job_name'])
    data = defaultdict(list)
    for job_name in warp_job_names:
        gwf_job = graph[job_name].gwf_job
        quantum = [_ for _ in gwf_job.quantum_graph.inputQuanta][0].quantum
        warp_dataId = quantum.dataId
        for key, values in quantum.inputs.items():
            if key.name == 'calexp':
                for item in values:
                    dataId = item.dataId
                    for field in 'band detector visit'.split():
                        data[field].append(dataId[field])
                    for field in 'tract patch'.split():
                        data[field].append(warp_dataId[field])
    return pd.DataFrame(data=data)
