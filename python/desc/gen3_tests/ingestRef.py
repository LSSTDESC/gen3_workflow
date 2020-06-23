#!/usr/bin/env python

from lsst.obs.lsst import LsstImSim
from lsst.daf.butler import Butler, DatasetType, FileDataset
from lsst.obs.base.gen2to3 import RootRepoConverter
from lsst.obs.base.gen2to3 import ConvertRepoTask

import lsst.daf.persistence as dafPersist

def makeTask(butler: Butler):
    instrument = LsstImSim()
    config = ConvertRepoTask.ConfigClass()
    instrument.applyConfigOverrides(ConvertRepoTask._DefaultName, config)
    config.instrument = "lsst.obs.lsst.LsstImSim"
    config.refCats = ["cal_ref_cat"]
    config.relatedOnly = True
    config.transfer = "symlink"
    config.datasetIncludePatterns = ["ref_cat"]
    return ConvertRepoTask(config=config, butler3=butler)

root3 = 'Run2.2i-gen3_24'
root2 = '/sps/lsst/data/boutigny/DC2/Run2.2i'
butler = Butler(root3, run="refcats")
task = makeTask(butler)
rootRepoConverter = RootRepoConverter(task=task, root=root2)
rootRepoConverter.prep() 
listOfFileDataset = list(rootRepoConverter.iterDatasets())

datasetType = DatasetType(name="cal_ref_cat", dimensions=["htm7"], storageClass="SimpleCatalog",
                              universe=butler.registry.dimensions)
butler.registry.registerDatasetType(datasetType)
butler.ingest(*listOfFileDataset) 