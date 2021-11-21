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

args = parser.parse_args()

df = query_workflow(args.workflow_name, db_file=args.db_file)
print_status(df)
