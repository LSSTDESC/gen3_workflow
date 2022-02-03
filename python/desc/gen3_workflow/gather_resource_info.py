"""
Code to gather resource usage information from per-task metadata.
"""
import sys
import re
from collections import defaultdict
import multiprocessing
import yaml
import numpy as np
import pandas as pd


__all__ = ['parse_metadata_yaml', 'gather_resource_info', 'add_num_visits']


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
        md = {} if md is None else md
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


def process_datarefs(datarefs, collections, butler, verbose=True):
    """
    Process a list of datarefs, extracting the per-task resource usage
    info from the `*_metadata` yaml files.
    """
    columns = ('detector', 'tract', 'patch', 'band', 'visit')
    data = defaultdict(list)
    nrefs = len(datarefs)
    for i, dataref in enumerate(datarefs):
        if verbose:
            if i % (nrefs//20) == 0:
                sys.stdout.write('.')
                sys.stdout.flush()
        yaml_file = butler.getURI(dataref, collections=collections).path
        data['task'].append(dataref.datasetType.name[:-len('_metadata')])
        dataId = dataref.expanded(dataref.dataId).dataId
        for column in columns:
            if column == 'visit' and 'visit' not in dataId:
                data[column].append(dataId.get('exposure', None))
            else:
                data[column].append(dataId.get(column, None))
        results = parse_metadata_yaml(yaml_file)
        data['cpu_time'].append(results.get('EndCpuTime', None))
        data['maxRSS'].append(results.get('MaxResidentSetSize', None))
    return pd.DataFrame(data=data)


def gather_resource_info(butler, dataId, collections=None, verbose=False,
                         datatype_pattern='.*_metadata', nmax=None,
                         processes=1):
    """
    Gather the per-task resource usage information from the
    `<task>_metadata` datasets.
    """
    registry = butler.registry
    pattern = re.compile(datatype_pattern)
    datarefs = list(registry.queryDatasets(pattern, dataId=dataId,
                                           findFirst=True,
                                           collections=collections))
    nrefs = len(datarefs)
    if verbose:
        print(f'found {nrefs} datarefs')
    nmax = nrefs if nmax is None else min(nrefs, nmax)

    if processes <= 1:
        return process_datarefs(datarefs, collections, butler, verbose=verbose)

    # Shuffle datarefs to help ensure the work is the same across
    # processes.
    np.random.shuffle(datarefs)
    datarefs = datarefs[:nmax]

    indexes = [int(_) for _ in np.linspace(0, nmax + 1, processes + 1)]

    with multiprocessing.Pool(processes=processes) as pool:
        futures = []
        for imin, imax in zip(indexes[:-1], indexes[1:]):
            verbose = verbose and (imin == indexes[0])
            args = datarefs[imin:imax], collections, butler, verbose
            futures.append(pool.apply_async(process_datarefs, args))
        pool.close()
        pool.join()
        dfs = [_.get() for _ in futures]
    df = pd.concat(dfs)
    return df


def add_num_visits(df, num_visits):
    """
    Add a column to the data frame from the gather_resource_info
    function with the number of visits for the relevant data selection
    using a ParslGraph.num_visits object.
    """
    data = []
    for _, row in df.iterrows():
        key = row.tract, row.patch
        band = row.band
        if band is not None:
            data.append(num_visits[key][band])
        elif row.task == 'deblend':
            data.append(sum(num_visits[key].values()))
        else:
            data.append(None)
    df['num_visits'] = data
    return df
