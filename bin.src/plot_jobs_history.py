#!/usr/bin/env python
"""
Script to plot the time history of the number of concurrent jobs running
for the specified workflow instance in a Parsl monitoring db file.
"""
import os
import sys
from collections import defaultdict
import argparse
import sqlite3
import matplotlib.pyplot as plt
from astropy.time import Time
import numpy as np
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('--workflow_name', type=str, default=None,
                    help='Name of workflow instance in the workflow table')
parser.add_argument('--db_file', type=str, default='monitoring.db',
                    help='Name of monitoring db file')
args = parser.parse_args()

if not os.path.isfile(args.db_file):
    raise FileNotFoundError(f'monitoring db file {args.db_file} not found')

if args.workflow_name is None:
    print(f'Available workflow names in {args.db_file}:')
    with sqlite3.connect(args.db_file) as conn:
        query = 'select distinct workflow_name from workflow'
        for workflow_name in pd.read_sql(query, conn)['workflow_name']:
            print(' ', workflow_name)
    sys.exit(0)

workflow_name = 'workQueue_csd3_monitoring_config.py'
db_file = 'monitoring-3828.db'

query = f'''select run_id, time_began from workflow where
            workflow_name="{args.workflow_name}"'''
with sqlite3.connect(args.db_file) as conn:
    df = pd.read_sql(query, conn)
    run_ids = df['run_id'].to_list()
    start_times = [_[:len('2021-05-20 11:33')].replace(' ', '_')
                   for _ in df['time_began']]

for run_id, start_time in zip(run_ids, start_times):
    query = f'''select task.task_stderr, status.task_status_name,
                status.timestamp
                from task join status on task.task_id=status.task_id and
                task.run_id=status.run_id join workflow
                on task.run_id=workflow.run_id where
                workflow.run_id="{run_id}"
                and task.task_stderr is not null
                order by task.task_stderr, status.timestamp desc'''

    with sqlite3.connect(args.db_file) as conn:
        df0 = pd.read_sql(query, conn)

    data = defaultdict(list)
    job_logs = set(df0.query('task_status_name == "running"')['task_stderr'])
    status_flags = set('running running_ended exec_done failed'.split())
    for _, row in df0.iterrows():
        if (row.task_status_name not in status_flags or
            row.task_stderr not in job_logs):
            continue
        job_name = os.path.basename(row.task_stderr)[:-len('.stderr')]
        data['job_name'].append(job_name)
        data['task'].append(job_name.split('_')[1])
        data['mjd'].append(Time(row.timestamp).mjd)
    df = pd.DataFrame(data=data)

    dt = 10/8.64e4   # Sample every 10 seconds
    bin_edges = np.arange(min(df['mjd']), max(df['mjd']) + dt, dt)
    tasks = np.array(list(set(df['task'])))
    bin_values = dict()
    for task in tasks:
        bin_values[task] = np.zeros(len(bin_edges)-1)

    job_names = set(df['job_name'])
    max_mjd = max(df['mjd'])
    for job_name in job_names:
        my_df = df.query(f'job_name == "{job_name}"')
        task = my_df['task'].to_numpy()[0]
        tmin = min(my_df['mjd'])
        tmax = max(my_df['mjd'])
        if tmax == tmin:
            # job is still running at current db state, so set job tmax to
            # the latest time entry in the dataframe.
            tmax = max_mjd
        index = np.where((tmin <= bin_edges) & (bin_edges < tmax))
        bin_values[task][index] += 1

    mean_times = []
    for task in tasks:
        mean_times.append(sum(bin_values[task]*bin_edges[:-1])
                          /sum(bin_values[task]))
    tasks = tasks[np.argsort(mean_times)]

    plt.figure()
    x = np.zeros(len(bin_edges) - 1)
    t0 = min(df['mjd'])
    edges = 24*60*(bin_edges - t0)
    for task in tasks:
        plt.stairs(bin_values[task], edges, label=task)
    plt.legend(fontsize='x-small')
    plt.xlabel(f'24*60*(mjd - {t0})')
    plt.ylabel('# concurrent jobs')
    plt.title(f'{workflow_name} {start_time}')
    plt.savefig(f'CSD3_DC2_3828_y1_{start_time}.png')
