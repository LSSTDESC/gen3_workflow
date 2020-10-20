from lsst.obs.base.gen2to3 import ConvertRepoTask
from lsst.obs.base import Instrument
from lsst.obs.lsst import LsstCamImSim
from lsst.daf.butler import Butler

def makeTask(butler: Butler, *,  fresh_start: bool = True):
    instrument = LsstCamImSim()
    config = ConvertRepoTask.ConfigClass()
    instrument.applyConfigOverrides(ConvertRepoTask._DefaultName, config)
    config.refCats = ["cal_ref_cat"]
    config.relatedOnly = True
    #config.transfer = "symlink"
    config.transfer = "auto"
    config.datasetIncludePatterns = ["flat", "bias", "dark", "fringe", "SKY",
#                                     "ref_cat",
                                     "raw"]
    config.datasetIgnorePatterns.append("*_camera")
    config.fileIgnorePatterns.extend(["*.log", "*.png", "rerun*"])
    config.doRegisterInstrument = fresh_start
    config.doWriteCuratedCalibrations = fresh_start
    return ConvertRepoTask(config=config, butler3=butler, instrument=instrument)

if __name__ == '__main__':
    instrument = LsstCamImSim()
    root2 = 'gen2-repo'
    root3 = 'gen3-repo'
    fresh_start = True
    butler = Butler(root3, run=instrument.makeDefaultRawIngestRunName())
    task = makeTask(butler, fresh_start=fresh_start)
    task.run(root=root2,
             reruns=[],
             calibs=({"CALIB": instrument.makeCollectionName("calib")}
                     if fresh_start else None))
