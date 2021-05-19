#!/usr/bin/env python
"""
Script to print a summary of workflow status.
"""
import argparse
from desc.gen3_workflow import query_workflow, print_status


parser = argparse.ArgumentParser(
    description='Print a summary of workflow status.')

parser.add_argument('workflow_name', type=str, help='workflow name')
parser.add_argument('--db_file', type=str, default='monitoring.db',
                    help='monitoring db filename')
parser.add_argument('--tasks', type=str, nargs='+', default='DRP',
                    help=('List of tasks in the workflow. By default, '
                          'the DRP tasks, listed in execution order, are '
                          'used. If set to "discover", the task names will be '
                          'found from the workflow and sorted alphabetically.'))

args = parser.parse_args()

tasks = args.tasks if args.tasks != 'DRP' else \
           ('isr characterizeImage calibrate skyCorrectionTask '
            'consolidateVisitSummary makeWarp selectGoodSeeingVisits '
            'assembleCoadd templateGen detection imageDifference '
            'mergeDetections deblend measure mergeMeasurements '
            'forcedPhotCoadd forcedPhotCcd forcedPhotDiffim'.split())

df = query_workflow(args.workflow_name, db_file=args.db_file)
print_status(df, tasks)
