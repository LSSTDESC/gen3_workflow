#!/usr/bin/env python
"""
Script to print a summary of workflow status.
"""
import os
import argparse
from desc.gen3_workflow import ParslGraph

parser = argparse.ArgumentParser(
    description='Print a summary of workflow status.')
parser.add_argument('workflow_name', type=str, help='workflow name')

args = parser.parse_args()

parsl_graph_file = os.path.join('submit', args.workflow_name,
                                'parsl_graph_config.pickle')
graph = ParslGraph.restore(parsl_graph_file, use_dfk=False)

graph.status()
