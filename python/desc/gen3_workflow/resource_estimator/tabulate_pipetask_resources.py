"""
Tabulate the computing resource usage for each DRP pipetask.
"""
import os
import time
from collections import defaultdict
import json
import numpy as np
import pandas as pd
from lsst.daf.butler import Butler, DimensionUniverse
from lsst.pipe.base.graph import QuantumGraph


__all__ = ['get_pipetask_resource_funcs', 'tabulate_pipetask_resources',
           'tabulate_data_product_sizes', 'total_node_hours']


class PipetaskFunc:
    """
    Functor class to package outputs of cpu_time and maxRSS functions.
    """
    def __init__(self, cpu_time, maxRSS):
        """
        Parameters
        ----------
        cpu_time : function
            Function to return cpu time in units of hours as a function
            of number of visits.
        maxRSS : function
            Function to return maximum RSS in units of GB as a function
            of number of visits.
        """
        self.cpu_time = cpu_time
        self.maxRSS = maxRSS

    def __call__(self, num_visits=1):
        """
        Parameters
        ----------
        num_visits : int [1]
            Number of visits to consider.  Not all functions depend
            meaningfully on the number of visits, e.g., the single
            frame processing, so a default of 1 is used.

        Returns
        -------
        tuple of (cpu_time (h), memory (GB))
        """
        return self.cpu_time(num_visits), self.maxRSS(num_visits)


def get_pipetask_resource_funcs(json_file, cpu_time_label='cpu_time (m)',
                                maxRSS_label='maxRSS (GB)',
                                cpu_time_factor=1./60., maxRSS_factor=1):
    """
    Read in the json file containing the functions describing the
    computing resource usage anticipated for each task as a function
    of number of visits.  These functions should return a tuple of cpu
    time (hours) and max RSS usage (GB).

    Parameters
    ----------
    json_file : str
        json file containing parameters describing the anticipated
        cpu time and memory used by the DRP pipetasks.
    cpu_time_label : str ['cpu_time (m)']
        Key name pointing to cpu time parameters.
    maxRSS_label : str ['maxRSS (GB)']
        Key name pointing to maximum RSS parameters.
    cpu_time_factor : float [1/60]
        Conversion factor to multiply the native units to convert to hours.
    maxRSS_factor : float [1]
        Conversion factor to multiply the native units to convert to GB.

    Returns
    -------
    dict of PipetaskFunc objects, keyed by task type.
    """
    with open(json_file) as fd:
        model_params = json.load(fd)
    pipetask_funcs = dict()
    for task, pars in model_params.items():
        cpu_time = np.poly1d(cpu_time_factor*np.array(pars[cpu_time_label]))
        maxRSS = np.poly1d(maxRSS_factor*np.array(pars[maxRSS_label]))
        pipetask_funcs[task] = PipetaskFunc(cpu_time, maxRSS)
    return pipetask_funcs


def tabulate_pipetask_resources(coadd_df, task_counts, pipetask_funcs,
                                num_visit_col='num_visits', verbose=False):
    """
    Tabulate the computing resources (cpu time, memory) for each
    of the pipetasks given a dataframe with the overlaps information.

    Parameters
    ----------
    coadd_df : pandas.DataFrame
        Dataframe with the number of visits for each band-tract-patch
        combination.
    task_counts : dict
        Dictionary of number of instances per task. Only counts for 'isr',
        'makeWarp', and 'assembleCoadd' are used.
    pipetask_funcs : dict
        Dictionary of functions, keyed by task type.  Each function should
        return a tuple (cpu_time in hours, memory usage in GB) taking
        the number of visits as the argument.
    num_visit_col : str ['num_visits']
        Column name in coadd_df that contains the number of visits per
        band-tract-patch coadd.
    verbose : bool [False]
        Verbosity flag.

    Returns
    -------
    pandas.DataFrame with the number of instances, total cpu time,
    and maximum and average memory used per pipetask.
    """
    pt_data = defaultdict(list)

    # sensor-visits:
    num_ccd_visits = task_counts['isr']
    for task_name in 'isr characterizeImage calibrate'.split():
        if verbose:
            print("processing", task_name)
        pt_data['pipetask'].append(task_name)
        pt_data['num_instances'].append(num_ccd_visits)
        cpu_hours, mem_GB = pipetask_funcs[task_name]()
        pt_data['cpu_hours'].append(cpu_hours*num_ccd_visits)
        pt_data['max_GB'].append(mem_GB)
        pt_data['avg_GB'].append(mem_GB)

    # warps:
    task_name = 'makeWarp'
    num_warps = task_counts[task_name]
    if verbose:
        print("processing", task_name)
    pt_data['pipetask'].append(task_name)
    pt_data['num_instances'].append(num_warps)
    cpu_hours, mem_GB = pipetask_funcs[task_name]()
    pt_data['cpu_hours'].append(cpu_hours*num_warps)
    pt_data['max_GB'].append(mem_GB)
    pt_data['avg_GB'].append(mem_GB)

    for task_name in ('assembleCoadd', 'detection', 'measure',
                      'forcedPhotCoadd'):
        if verbose:
            print("processing", task_name, end=' ')
        pt_data['pipetask'].append(task_name)
        pt_data['num_instances'].append(len(coadd_df))
        cpu_hours_total = 0
        memory = []
        t0 = time.time()
        for _, row in coadd_df.iterrows():
            num_visits = row[num_visit_col]
            cpu_hours, mem_GB = pipetask_funcs[task_name](num_visits)
            cpu_hours_total += cpu_hours
            memory.append(mem_GB)
        pt_data['cpu_hours'].append(cpu_hours_total)
        pt_data['max_GB'].append(np.max(memory))
        pt_data['avg_GB'].append(np.mean(memory))
        if verbose:
            print(time.time() - t0)

    for task_name in ('mergeCoaddDetections', 'deblend',
                      'mergeCoaddMeasurements'):
        try:
            cpu_hours, mem_GB = pipetask_funcs[task_name]()
        except KeyError:
            continue
        if verbose:
            print("processing", task_name)
        pt_data['pipetask'].append(task_name)
        num_instances = int(len(coadd_df)/6)
        pt_data['num_instances'].append(num_instances)
        pt_data['cpu_hours'].append(num_instances*cpu_hours)
        pt_data['max_GB'].append(mem_GB)
        pt_data['avg_GB'].append(mem_GB)

    return pd.DataFrame(data=pt_data)


def tabulate_data_product_sizes(qgraph_file, repo, collection):
    """
    Tabulate the mean sizes of data products listed in a QuantumGraph
    using files in a given repo and collection.

    Parameters
    ----------
    qgraph_file : str
        QuantumGraph file produced by `pipetask qgraph`.
    repo : str
        Path to data repository.
    collection : str
        Collection in repo to use for finding example data products.

    Returns
    -------
    dict(dict(tuple)) Outer dict keyed by task label, inner dicts keyed
    by dataset type with tuple of (mean file size (GB), std file sizes (GB),
    number of files in examples).
    """
    qgraph = QuantumGraph.loadUri(qgraph_file)

    butler = Butler(repo, collections=[collection])
    registry = butler.registry

    # Traverse the QuantumGraph finding the dataset types associated
    # with each task type.
    dstypes = defaultdict(set)
    for node in qgraph:
        task = node.taskDef.label
        for dstype in node.quantum.outputs:
            dstypes[task].add(dstype.name)

    # Loop over task types and query for each dataset types and
    # compute mean and stdev file sizes for each dataset type.
    data = defaultdict(dict)
    for task, dstypes in dstypes.items():
        for dstype in dstypes:
            file_sizes = [os.stat(butler.getURI(_).path).st_size/1024**3
                          for _ in registry.queryDatasets(dstype)]
            data[task][dstype] = (np.nanmean(file_sizes), np.nanstd(file_sizes),
                                  len(file_sizes))
    return data


def total_node_hours(pt_df, cpu_factor=8, cores_per_node=68,
                     memory_per_node=96, memory_min=10):
    """
    Estimate the total number of node hours to do an image processing
    run.

    Parameters
    ----------
    pt_df : pandas.DataFrame
        DataFrame containing the number of instances, total cpu time,
        and maximum and average memory used per pipetask.  This is
        the output of `tabulate_pipetask_resources`.
    cpu_factor : float [8]
        Slow down factor to apply to the pipetask cpu times.  The
        cpu times were derived from Cori-Haswell runs and the default
        value of 8 is the empirically observed slow-down for running
        the same cpu-bound job on a Cori-KNL node.
    cores_per_node : int [68]
        Number of cores per node.  68 is the Cori-KNL value.
    memory_per_node : int [96]
        Memory per node in GB.  96 is the Cori-KNL value.
    memory_min : int [10]
        Memory in GB to reserve per node as a safety factor.  10GB is
        a conservative number for these jobs.

    Returns
    -------
    tuple(float, float): The first entry, `node_hours`, is computed
    using the maximum memory estimate per process to determine the
    number of cores per node for a given pipe task ; the second entry,
    `node_hours_opt`, is an optimistic estimate using the average
    memory per process.
    """
    available_memory = memory_per_node - memory_min
    node_hours = 0
    node_hours_opt = 0
    for _, row in pt_df.iterrows():
        ncores = min(cores_per_node, int(available_memory/row['max_GB']))
        ncores_avg = min(cores_per_node, int(available_memory/row['avg_GB']))
        node_hours += row['cpu_hours']*cpu_factor/ncores
        node_hours_opt += row['cpu_hours']*cpu_factor/ncores_avg
    return node_hours, node_hours_opt
