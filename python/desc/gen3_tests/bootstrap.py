#!/usr/bin/env python

from __future__ import annotations

import shutil
import os
from typing import List

import lsst.log.utils
from lsst.obs.base.gen2to3 import ConvertRepoTask
from lsst.obs.base import Instrument
from lsst.obs.lsst import LsstImSim
from lsst.daf.butler import Butler, DatasetType
from lsst.pipe.tasks.makeSkyMap import MakeSkyMapTask

VISITS = {
    3078: {
    	"u": [
    		 179239, 180007, 200934, 217677, 2348, 235896, 235897, 235898, 
    		 277064, 277065
    	],
        "g": [
            159469, 185733, 193777, 193810, 193811, 193819, 221618, 400421, 
            400422, 400457
        ],
        "r": [
            185772, 185820, 193190, 193236, 193802, 193837, 202586, 212242,
            212243, 212703
        ],
        "i": [
            184839, 184884, 192357, 204713, 211131, 211133, 211134, 214436,
            214471, 227985
        ],
        "z": [
            13289, 209058, 209066, 209091, 237907, 240855, 303558, 32683,
            426663, 443964
        ],
        "y": [
            166960, 167880, 169761, 169766, 169768, 169814, 189283, 190289,
            190612, 190624
        ],
    },
}

GEN2_RAW_ROOT = "/sps/lsst/data/boutigny/DC2/Run2.2i"


def configureLogging(level):
    lsst.log.configure_prop("""
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.out
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern=%-5p %d{yyyy-MM-ddTHH:mm:ss.SSSZ} %c - %m%n
""")
    log = lsst.log.Log.getLogger("convertRepo")
    log.setLevel(level)


def makeVisitList(tracts: List[int], filters: List[str]):
    visits = []
    for tract in tracts:
        for filter in filters:
            visits.extend(VISITS[tract][filter])
    return visits

def makeTask(butler: Butler, *, continue_: bool = False):
    instrument = LsstImSim()
    config = ConvertRepoTask.ConfigClass()
    instrument.applyConfigOverrides(ConvertRepoTask._DefaultName, config)
    config.instrument = "lsst.obs.lsst.LsstImSim"
    config.relatedOnly = True
    #config.transfer = "symlink"
    config.transfer = "auto"
    config.datasetIncludePatterns = ["flat", "bias", "dark", "fringe", "SKY",
                                     "ref_cat", "raw"]
    config.datasetIgnorePatterns.append("*_camera")
    config.fileIgnorePatterns.extend(["*.log", "*.png", "rerun*"])
    config.doRegisterInstrument = not continue_
    config.doWriteCuratedCalibrations = not continue_
    return ConvertRepoTask(config=config, butler3=butler)


def putSkyMap(butler: Butler, instrument: Instrument):
    datasetType = DatasetType(name="deepCoadd_skyMap", dimensions=["skymap"], storageClass="SkyMap",
                              universe=butler.registry.dimensions)
    butler.registry.registerDatasetType(datasetType)
    run = "skymaps"
    butler.registry.registerRun(run)
    skyMapConfig = MakeSkyMapTask.ConfigClass()
    instrument.applyConfigOverrides(MakeSkyMapTask._DefaultName, skyMapConfig)
    skyMap = skyMapConfig.skyMap.apply()
    butler.put(skyMap, datasetType, skymap="rings", run=run)


def run(root: str, *, tracts: List[int], filters: List[str], instrument: Instrument,
        create: bool = False, clobber: bool = False, continue_: bool = False):
    if create:
        if continue_:
            raise ValueError("Cannot continue if create is True.")
        if os.path.exists(root):
            if clobber:
                shutil.rmtree(root)
            else:
                raise ValueError("Repo exists and --clobber=False.")
        Butler.makeRepo(root)
    butler = Butler(root, run=instrument.makeDefaultRawIngestRunName())
    task = makeTask(butler, continue_=continue_)
    task.run(
        root=GEN2_RAW_ROOT,
        #reruns=['run2.2i-coadd-wfd-dr6-v1'],
        reruns=[],
        calibs=({"CALIB": instrument.makeCollectionName("calib")} if not continue_ else None),
        visits=makeVisitList(tracts, filters)
    )
    #if not continue_:
    #    task.log.info("Writing deepCoadd_skyMap.")
    #   putSkyMap(butler, task.instrument)


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Bootstrap a Gen3 Butler data repository with DC2 imsim Run2.2i data."
    )
    parser.add_argument("root", type=str, metavar="PATH",
                        help="Gen3 repo root (or config file path.)")
    parser.add_argument("--create", action="store_true", default=False,
                        help="Create the repo before attempting to populating it.")
    parser.add_argument("--clobber", action="store_true", default=False,
                        help="Remove any existing repo if --create is passed (ignored otherwise).")
    parser.add_argument("--continue", dest="continue_", action="store_true", default=False,
                        help="Ingest more tracts and/or filters into a repo already containing calibs.")
    parser.add_argument("--tract", type=int, action="append",
                        help=("Ingest raws from this tract (may be passed multiple times; "
                              "default is all known tracts)."))
    parser.add_argument("--filter", type=str, action="append", choices=("u", "g", "r", "i", "z", "y"),
                        help=("Ingest raws from this filter (may be passed multiple times; "
                              "default is ugrizy)."))
    parser.add_argument("-v", "--verbose", action="store_const", dest="verbose",
                        default=lsst.log.Log.INFO, const=lsst.log.Log.DEBUG,
                        help="Set the log level to DEBUG.")
    options = parser.parse_args()
    tracts = options.tract if options.tract else list(VISITS.keys())
    filters = options.filter if options.filter else list("ugrizy")
    configureLogging(options.verbose)
    run(options.root, tracts=tracts, filters=filters, create=options.create, clobber=options.clobber,
        continue_=options.continue_, instrument=LsstImSim())


if __name__ == "__main__":
    main()
