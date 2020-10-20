from lsst.obs.lsst import LsstCamImSim
from lsst.daf.butler import Butler, DatasetType, FileDataset
from lsst.obs.base.gen2to3 import RootRepoConverter
from lsst.obs.base.gen2to3 import ConvertRepoTask

import lsst.daf.persistence as dafPersist

def makeTask(butler: Butler):
    instrument = LsstCamImSim()
    config = ConvertRepoTask.ConfigClass()
    instrument.applyConfigOverrides(ConvertRepoTask._DefaultName, config)
    #config.instrument = "lsst.obs.lsst.LsstCamImSim"
    config.refCats = ["cal_ref_cat"]
    config.relatedOnly = True
    config.transfer = "symlink"
    config.datasetIncludePatterns = ["ref_cat"]
    return ConvertRepoTask(config=config, butler3=butler, instrument=instrument)

root3 = 'gen3-repo'
root2 = 'gen2-repo'
butler = Butler(root3, run="refcats")
task = makeTask(butler)
instrument = LsstCamImSim()
rootRepoConverter = RootRepoConverter(task=task, root=root2,
                                      instrument=instrument)
rootRepoConverter.prep()
listOfFileDataset = list(rootRepoConverter.iterDatasets())

datasetType = DatasetType(name="cal_ref_cat", dimensions=["htm7"],
                          storageClass="SimpleCatalog",
                          universe=butler.registry.dimensions)
butler.registry.registerDatasetType(datasetType)
butler.ingest(*listOfFileDataset)
