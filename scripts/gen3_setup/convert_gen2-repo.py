from lsst.obs.base.gen2to3 import ConvertRepoTask
from lsst.obs.base import Instrument
from lsst.obs.lsst import LsstImSim
from lsst.daf.butler import Butler

def makeTask(butler: Butler, *, continue_: bool = False):
    instrument = LsstImSim()
    config = ConvertRepoTask.ConfigClass()
    instrument.applyConfigOverrides(ConvertRepoTask._DefaultName, config)
    #config.instrument = "lsst.obs.lsst.LsstImSim"
    config.relatedOnly = True
    #config.transfer = "symlink"
    config.transfer = "auto"
    config.datasetIncludePatterns = ["flat", "bias", "dark", "fringe", "SKY",
                                     "ref_cat", "raw"]
    config.datasetIgnorePatterns.append("*_camera")
    config.fileIgnorePatterns.extend(["*.log", "*.png", "rerun*"])
    config.doRegisterInstrument = not continue_
    config.doWriteCuratedCalibrations = not continue_
    return ConvertRepoTask(config=config, butler3=butler, instrument=instrument)

if __name__ == '__main__':
    instrument = LsstImSim()
    root = 'gen3-repo'
    continue_ = False
    butler = Butler(root, run=instrument.makeDefaultRawIngestRunName())
    task = makeTask(butler, continue_=continue_)
    task.run(root='gen2-repo',
             reruns=[],
             calibs=({"CALIB": instrument.makeCollectionName("calib")}
                     if not continue_ else None))
