"""
Code to gather resource usage information from per-task metadata.
"""
import os
import sys
import re
import glob
import time
from collections import defaultdict
import yaml
import numpy as np
import pandas as pd
from lsst.daf.butler import Butler
from lsst.daf.base import PropertyList, PropertySet


__all__ = ['parse_metadata_yaml', 'gather_resource_info']


def parse_metadata_yaml(yaml_file):
    """
    Parse the runtime and RSS data in the metadata yaml file created
    by the lsst.pipe.base.timeMethod decorator as applied to pipetask
    methods.
    """
    time_types = 'Cpu User System'.split()
    min_fields = [f'Start{_}Time' for _ in time_types]
    max_fields = [f'End{_}Time' for _ in time_types] + ['MaxResidentSetSize']

    results = dict()
    with open(yaml_file) as fd:
        md = yaml.safe_load(fd)
    methods = list(md.keys())
    for method in methods:
        for key, value in md[method].items():
            for min_field in min_fields:
                if not key.endswith(min_field):
                    continue
                if min_field not in results or value < results[min_field]:
                    results[min_field] = value
                    continue
            for max_field in max_fields:
                if not key.endswith(max_field):
                    continue
                if max_field not in results or value > results[max_field]:
                    results[max_field] = value
                    continue
    return results


def gather_resource_info(butler, dataId, collections=None, verbose=False):
    """
    Gather the per-task resource usage information from the
    `<task>_metadata` datasets.
    """
    columns = ('detector', 'tract', 'patch', 'band', 'visit')
    registry = butler.registry
    data = defaultdict(list)
    pattern = re.compile('.*_metadata')
    datarefs = registry.queryDatasets(pattern, dataId=dataId, findFirst=True,
                                      collections=collections)
    if verbose:
        print(f'found {len(list(datarefs))} datarefs')
    yaml_files = set()
    nrefs = len(list(datarefs))
    for i, dataref in enumerate(datarefs):
        if verbose:
            if i % (nrefs//20) == 0:
                sys.stdout.write('.')
                sys.stdout.flush()
        yaml_file = butler.getURI(dataref, collections=collections).path
        if yaml_file not in yaml_files:
            yaml_files.add(yaml_file)
        else:
            continue
        data['task'].append(dataref.datasetType.name[:-len('_metadata')])
        dataId = dict(dataref.dataId)
        if 'visit' not in dataId and 'exposure' in dataId:
            dataId['visit'] = dataId['exposure']
        for column in columns:
            data[column].append(dataId.get(column, None))
        results = parse_metadata_yaml(yaml_file)
        data['cpu_time'].append(results.get('EndCpuTime', None))
        data['maxRSS'].append(results.get('MaxResidentSetSize', None))
    if verbose:
        print()

    return pd.DataFrame(data=data)
