"""
Tabulate the computing resource usage for each DRP pipe_task.
"""
from collections import defaultdict
import numpy as np
import pandas as pd
from .pipe_task_resource_usage import pipe_tasks


__all__ = ['tabulate_pipe_task_resources', 'total_node_hours']


def tabulate_pipe_task_resources(coadd_df, num_visit_col='num_visits',
                                 num_ccd_visits=None, verbose=False):
    """
    Tabulate the computing resources (cpu time, memory) for each
    of the pipe_tasks given a dataframe with the overlaps information.

    Parameters
    ----------
    coadd_df : pandas.DataFrame
        Dataframe with the number of vists for each band-tract-patch
        combination.
    num_visit_col : str ['num_visits']
        Column name in coadd_df that contains the number of visits per
        band-tract-patch coadd.
    num_ccd_visits : int [None]
        Number of ccd-visits, e.g., the number of `isr` tasks in a QG.
        If None, then skip visit-level tasks like `isr`, `characterizeImage`,
        and `calibrate`.
    verbose : bool [False]
        Verbosity flag.

    Returns
    -------
    pandas.DataFrame with the number of instances, total cpu time,
    and maximum and average memory used per pipe_task.
    """
    pt_data = defaultdict(list)

    # sensor-visits:
    if num_ccd_visits is not None:
        for task_name in 'isr characterizeImage calibrate'.split():
            if verbose:
                print("processing", task_name)
            pt_data['pipe_task'].append(task_name)
            pt_data['num_instances'].append(num_ccd_visits)
            cpu_hours, mem_GB = pipe_tasks[task_name]()
            pt_data['cpu_hours'].append(cpu_hours*num_sensor_visits)
            pt_data['max_GB'].append(mem_GB)
            pt_data['avg_GB'].append(mem_GB)

    # warps:
    task_name = 'makeWarp'
    if verbose:
        print("processing", task_name)
    pt_data['pipe_task'].append(task_name)
    num_warps = len(unique_tuples(df, 'visit tract patch'.split()))
    pt_data['num_instances'].append(num_warps)
    cpu_hours, mem_GB = pipe_tasks[task_name]()
    pt_data['cpu_hours'].append(cpu_hours*num_warps)
    pt_data['max_GB'].append(mem_GB)
    pt_data['avg_GB'].append(mem_GB)

    for task_name in ('assembleCoadd', 'templateGen', 'detection', 'measure',
                      'forcedPhotCoadd'):
        if verbose:
            print("processing", task_name)
        pt_data['pipe_task'].append(task_name)
        pt_data['num_instances'].append(len(coadd_df))
        cpu_hours_total = 0
        memory = []
        for _, row in coadd_df.iterrows():
            num_visits = row['num_visits']
            cpu_hours, mem_GB = pipe_tasks[task_name](num_visits)
            cpu_hours_total += cpu_hours
            memory.append(mem_GB)
        pt_data['cpu_hours'].append(cpu_hours_total)
        pt_data['max_GB'].append(np.max(memory))
        pt_data['avg_GB'].append(np.mean(memory))

    for task_name in ('mergeCoaddDetections', 'mergeCoaddMeasurements'):
        if verbose:
            print("processing", task_name)
        pt_data['pipe_task'].append(task_name)
        num_instances = int(len(coadd_df)/6)
        pt_data['num_instances'].append(num_instances)
        cpu_hours, mem_GB = pipe_tasks[task_name]()
        pt_data['cpu_hours'].append(num_instances*cpu_hours)
        pt_data['max_GB'].append(mem_GB)
        pt_data['avg_GB'].append(mem_GB)

    # deblend
    task_name = 'deblend'
    if verbose:
        print("processing", task_name)
    pt_data['pipe_task'].append(task_name)
    num_instances = int(len(coadd_df)/6)
    pt_data['num_instances'].append(num_instances)
    cpu_hours_total = 0
    memory = []
    for band in 'ugrizy':
        my_df
    for _, row in coadd_df.iterrows():
        num_visits = row['num_visits']
        cpu_hours, mem_GB = pipe_tasks[task_name](num_visits)
        
    cpu_hours, mem_GB = pipe_tasks[task_name]()
    pt_data['cpu_hours'].append(cpu_hours*num_warps)
    pt_data['max_GB'].append(mem_GB)
    pt_data['avg_GB'].append(mem_GB)
    

    return pd.DataFrame(data=pt_data)


def total_node_hours(pt_df, cpu_factor=8, cores_per_node=68,
                     memory_per_node=96, memory_min=10):
    """
    Estimate the total number of node hours to do an image processing
    run.

    Parameters
    ----------
    pt_df : pandas.DataFrame
        DataFrame containing the number of instances, total cpu time,
        and maximum and average memory used per pipe_task.  This is
        the output of `tabulate_pipe_task_resources`.
    cpu_factor : float [8]
        Slow down factor to apply to the pipe_task cpu times.  The
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
