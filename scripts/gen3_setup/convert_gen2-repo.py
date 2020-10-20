from lsst.daf.butler import Butler, DatasetType, FileDataset
from lsst.obs.base.gen2to3 import ConvertRepoTask, RootRepoConverter
from lsst.obs.lsst import LsstCamImSim


def makeRawCalibConvertTask(butler: Butler, fresh_start: bool=True):
    instrument = LsstCamImSim()
    config = ConvertRepoTask.ConfigClass()
    instrument.applyConfigOverrides(ConvertRepoTask._DefaultName, config)
    config.relatedOnly = True
    config.transfer = "auto"
    config.datasetIncludePatterns = ["flat", "bias", "dark", "fringe", "SKY",
                                     "raw"]
    config.datasetIgnorePatterns.append("*_camera")
    config.fileIgnorePatterns.extend(["*.log", "*.png", "rerun*"])
    config.doRegisterInstrument = fresh_start
    config.doWriteCuratedCalibrations = fresh_start
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
    instrument = LsstCamImSim()
    root2 = 'gen2-repo'
    root3 = 'gen3-repo-test'

    # Convert raw files and CALIBs
    fresh_start = True
    butler = Butler(root3, run=instrument.makeDefaultRawIngestRunName())
    task = makeRawCalibConvertTask(butler, fresh_start=fresh_start)
    task.run(root=root2, reruns=[],
             calibs=({"CALIB": instrument.makeCollectionName("calib")}
                     if fresh_start else None))

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
