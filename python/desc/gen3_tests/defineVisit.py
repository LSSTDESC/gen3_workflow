#!/usr/bin/env python

from lsst.daf.butler import Butler 
from lsst.obs.base import DefineVisitsTask, DefineVisitsConfig
from lsst.obs.base.utils import getInstrument

def defineVisits(repo, config_file, collection, instrument, exposure):
	butler = Butler(repo, collections=collection, writeable=True)
	config = DefineVisitsConfig()
	instr = getInstrument(instrument, butler.registry)
	instr.applyConfigOverrides(DefineVisitsTask._DefaultName, config)

	if config_file is not None:
		config.load(config_file)
	task = DefineVisitsTask(config=config, butler=butler)
	res = butler.registry.queryDimensions(["exposure"], where="exposure = %d"%exposure) 
	task.run(res)

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Define a single visit in gen3"
    )
    parser.add_argument("root", type=str, metavar="PATH",
                        help="Gen3 repo root")
    parser.add_argument("--config", type=str, action="append",
                        help="config config_file")
    parser.add_argument("--collection", type=str, action="store",
                        help="collection name")
    parser.add_argument("--instrument", type=str, action="store",
                        help="instrument - default lsst.obs.lsst.LsstImSim")
    parser.add_argument("--visit", type=int, action="store",
                        help="visit number")

    options = parser.parse_args()
    instrument = options.instrument if options.instrument else "lsst.obs.lsst.LsstImSim"
    config_file = options.config
    root = options.root
    collection = options.collection
    visit = options.visit

    print(root, collection, visit)

    defineVisits(root, config_file, collection, instrument, visit)

if __name__ == "__main__":
    main()