"""
Script to export DRP output collections from a Gen3 repo.

The exported data can be imported into an existing repo using

butler import <full path to destination repo> <full path to source repo> \
    --export-file <full path to export file>

The destination repo is assumed to have the same basline input collections
(raw files, calibs, ref cats) as the source repo.  Note that using relative
paths with the `butler import` command can cause problems.
"""
import argparse
import lsst.daf.butler as daf_butler

parser = argparse.ArgumentParser(
    description='Script to export DRP output collections from a Gen3 repo.')
parser.add_argument('repo', type=str, help='Source repository')
parser.add_argument('--collections', type=str, nargs='+',
                    help='Collections to export.')
parser.add_argument('--export_file', type=str, default='export.yaml',
                    help='Name of export file.')

args = parser.parse_args()
butler = daf_butler.Butler(args.repo)
with butler.export(filename=args.export_file) as export:
    datasets = butler.registry.queryDatasets(datasetType=... ,
                                             collections=args.collections)
    export.saveDatasets(datasets, elements=())
