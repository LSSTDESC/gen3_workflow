"""
Module to extract status info of the workflow tasks from the
monitoring.db file.
"""
import os
from collections import defaultdict
import sqlite3
import numpy as np
import pandas as pd


__all__ = ['query_workflow', 'print_status', 'DRP_TASKS']


DRP_TASKS = tuple('isr characterizeImage calibrate skyCorrectionTask '
                  'consolidateVisitSummary makeWarp selectGoodSeeingVisits '
                  'assembleCoadd templateGen detection imageDifference '
                  'mergeDetections deblend measure mergeMeasurements '
                  'forcedPhotCoadd forcedPhotCcd forcedPhotDiffim'.split())


def query_workflow(workflow_name, db_file='monitoring.db'):
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
            raise FileNotFoundError(f'workflow {workflow_name} not in {db_file}')
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
    task_stderrs = sorted(list(set(df0['task_stderr'])))
    for task_stderr in task_stderrs:
        df = df0.query(f'task_stderr == "{task_stderr}"')
        #index = np.argsort(df['timestamp'])
        #status = df['task_status_name'].to_numpy()[index][-1]
        status = df['task_status_name'].to_numpy()[0]
        job_name = os.path.basename(task_stderr).split('.')[0]
        data['job_name'].append(job_name)
        data['task_type'].append(job_name.split('_')[1])
        data['status'].append(status)
    return pd.DataFrame(data=data)


def print_status(df, task_types=DRP_TASKS):
    """
    Given a dataframe from `query_workflow(...)` and a list of task types,
    print the numbers of each task types for each status value.
    """
    # Get the task type names from the dataframe.
    actual_task_types = set(df['task_type'])
    if task_types[0] == 'discover':
        # Use the discovered task types and sort.
        task_types = sorted(list(actual_task_types))
    # Make sure task_types list includes all actual task types.
    task_types.extend(actual_task_types.difference(task_types))
#    statuses = ('pending launched running running_ended exec_done '
#                'failed dep_fail'.split())
    statuses = 'pending launched running exec_done failed dep_fail'.split()
    spacer = ' '
    print(f'{"task_type":25}', end=spacer)
    for status in statuses:
        print(f'{status:>10}', end=spacer)
    print(f'{"total":>10}')
    for task_type in task_types:
        if task_type not in actual_task_types:
            continue
        print(f'{task_type:25}', end=spacer)
        df1 = df.query(f'task_type == "{task_type}"')
        for status in statuses:
            df2 =  df1.query(f'status == "{status}"')
            print(f'{len(df2):10d}', end=spacer)
        print(f'{len(df1):10d}')
