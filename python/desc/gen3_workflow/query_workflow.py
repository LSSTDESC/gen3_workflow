"""
Module to extract status info of the workflow tasks from the
monitoring.db file.
"""
import os
from collections import defaultdict
import sqlite3
import numpy as np
import pandas as pd


__all__ = ['query_workflow', 'print_status', 'get_task_name']


def is_uuid(value):
    """Check if the passed value is formatted like a UUID."""
    sizes = tuple([len(_) for _ in value.split('-')])
    return sizes == (8, 4, 4, 4, 12)


def get_task_name(job_name, bps_config=None):
    """Extract the task name from the GenericWorkflowJob name."""
    # Get cluster names from any quantum clustering specification
    # in the bps config yaml.
    tokens = job_name.split('_')
    if bps_config is not None:
        cluster_names = list(bps_config['cluster'].keys())
        if tokens[0] in cluster_names:
            # In case of quantum clustering, we use the cluster name as
            # the task name.
            return tokens[0]
    # If bps_config is None or if the tokens[0] is not in
    # cluster_names, then check if it is formatted like a uuid,
    # in which case tokens[1] is the task name.
    if is_uuid(tokens[0]):
        return tokens[1]
    # Finally, for backwards compatibility with weeklies prior to
    # w_2022_01, check if tokens[0] can be cast as an int.  If not,
    # then it's the cluster name.
    try:
        _ = int(tokens[0])
    except ValueError:
        return tokens[0]

    return tokens[1]


def query_workflow(workflow_name, db_file='./runinfo/monitoring.db'):
    """
    Query the workflow, task, and status tables for the
    status of each task.  Use the task.task_stderr as the unique
    identifier of each task.
    """
    if not os.path.isfile(db_file):
        raise FileNotFoundError(db_file)
    with sqlite3.connect(db_file) as conn:
        df = pd.read_sql('select * from workflow where '
                         f'workflow_name="{workflow_name}"', conn)
        if df.empty:
            raise FileNotFoundError(f'workflow {workflow_name}'
                                    'not in {db_file}')
    query = f'''select task.task_stderr, status.task_status_name,
                status.timestamp
                from task join status on task.task_id=status.task_id and
                task.run_id=status.run_id join workflow
                on task.run_id=workflow.run_id where
                workflow.workflow_name="{workflow_name}"
                and task.task_stderr is not null
                order by task.task_stderr, status.timestamp desc'''

    with sqlite3.connect(db_file) as conn:
        df0 = pd.read_sql(query, conn)

    data = defaultdict(list)
    task_stderrs = set()
    for _, row in df0.iterrows():
        if (row['task_stderr'] in task_stderrs and
           row['task_status_name'] != "exec_done"):
            continue
        task_stderrs.add(row['task_stderr'])
        job_name = os.path.basename(row['task_stderr']).split('.')[0]
        data['job_name'].append(job_name)
        task_type = get_task_name(job_name)
        data['task_type'].append(task_type)
        data['status'].append(row['task_status_name'])
    if not data:
        # No tasks have been processed yet, so return an empty dataframe.
        return pd.DataFrame()
    idx = np.array(data['status']) != "running_ended"
    return pd.DataFrame(data=data)[idx]


def print_status(df, task_types=None):
    """
    Given a dataframe from `query_workflow(...)` and a list of task types,
    print the numbers of each task types for each status value.
    """
    if task_types is None:
        task_types = sorted(list(set(df['task_type'])))
    wtt = 8
    for task_type in task_types:
        if len(task_type) > wtt:
            wtt = len(task_type)
#    statuses = ('pending launched running running_ended exec_done '
#                'failed dep_fail'.split())
    statuses = 'pending launched running exec_done failed dep_fail'.split()
    spacer = ' '
    print(f'{"task_type":{wtt}}', end=spacer)
    for status in statuses:
        print(f'{status:>10}', end=spacer)
    print(f'{"total":>10}')
    for task_type in task_types:
        print(f'{task_type:{wtt}}', end=spacer)
        df1 = df.query(f'task_type == "{task_type}"')
        for status in statuses:
            df2 = df1.query(f'status == "{status}"')
            print(f'{len(df2):10d}', end=spacer)
        print(f'{len(df1):10d}')
