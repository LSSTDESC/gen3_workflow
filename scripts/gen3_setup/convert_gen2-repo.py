#!/usr/bin/env python
import os
import lsst.utils
import lsst.daf.butler as daf_butler
from lsst.daf.butler import Butler, DatasetType, FileDataset
from lsst.obs.base.gen2to3 import ConvertRepoTask, RootRepoConverter, \
    CalibRepo
from lsst.pipe.tasks.script.registerSkymap import registerSkymap
from lsst.obs.lsst import LsstCamImSim


def makeRawCalibConvertTask(butler: Butler, fresh_start: bool=True):
    instrument = LsstCamImSim()
    config = ConvertRepoTask.ConfigClass()
    instrument.applyConfigOverrides(ConvertRepoTask._DefaultName, config)
    config.relatedOnly = True
    config.transfer = "symlink"
    config.datasetIncludePatterns = ["flat", "bias", "dark", "fringe", "sky",
                                     "raw"]
    config.datasetIgnorePatterns.append("*_camera")
    config.fileIgnorePatterns.extend(["*.log", "*.png", "rerun*"])
    config.doRegisterInstrument = fresh_start
    return ConvertRepoTask(config=config, butler3=butler, instrument=instrument)


def makeRefCatConvertTask(butler: Butler):
    instrument = LsstCamImSim()
    config = ConvertRepoTask.ConfigClass()
    instrument.applyConfigOverrides(ConvertRepoTask._DefaultName, config)
    config.refCats = ["cal_ref_cat"]
    config.relatedOnly = True
    config.transfer = "symlink"
    config.datasetIncludePatterns = ["ref_cat"]
    return ConvertRepoTask(config=config, butler3=butler, instrument=instrument)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description=("Convert a Gen2 repo containing only raw files, "
                     "calibs, and ref cats to Gen3"))
    parser.add_argument('gen2_repo', type=str, help='path to Gen2 repo')
    parser.add_argument('gen3_repo', type=str,
                        help='empty Gen3 repo created with `butler create`')
    args = parser.parse_args()

    root2 = os.path.abspath(args.gen2_repo)
    root3 = os.path.abspath(args.gen3_repo)
    if not os.path.isdir(root3):
        raise RuntimeError(f'{args.gen3_repo} does not exist.  '
                           f'Create it with `butler create ...`')

    instrument = LsstCamImSim()

    # Convert raw files and CALIBs
    fresh_start = True
    butler = Butler(root3, run=instrument.makeDefaultRawIngestRunName())
    task = makeRawCalibConvertTask(butler, fresh_start=fresh_start)
    calib_path = os.path.join(root2, 'CALIB')
    task.run(root=root2, reruns=[], calibs=[CalibRepo(path=calib_path)])

    # Convert ref_cats
    butler = Butler(root3, run='ref_cat')
    task = makeRefCatConvertTask(butler)
    rootRepoConverter = RootRepoConverter(task=task, root=root2,
                                          instrument=instrument)
    rootRepoConverter.prep()
    listOfFileDataset = list(rootRepoConverter.iterDatasets())
    datasetType = DatasetType(name='cal_ref_cat', dimensions=['htm7'],
                              storageClass='SimpleCatalog',
                              universe=butler.registry.dimensions)
    butler.registry.registerDatasetType(datasetType)
    butler.ingest(*listOfFileDataset)

    # Register the DC2 skymap.
    config = None
    config_file = os.path.join(lsst.utils.getPackageDir('gen3_workflow'),
                               'config', 'makeSkyMap.py')
    registerSkymap(root3, config, config_file)

    # Set the default collection to 'LSSTCam-imSim/defaults'.
    butler = Butler(root3, writeable=True)
    registry = butler.registry

    default_collection = 'LSSTCam-imSim/defaults'

    if default_collection in list(registry.queryCollections()):
        registry.removeCollection(default_collection)

    registry.registerCollection(default_collection,
                                type=daf_butler.CollectionType.CHAINED)
    registry.setCollectionChain(default_collection,
                                ['LSSTCam-imSim/raw/all',
                                 'LSSTCam-imSim/calib',
                                 'skymaps', 'ref_cat'])
