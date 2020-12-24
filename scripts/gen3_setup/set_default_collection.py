import lsst.daf.butler as daf_butler

butler = daf_butler.Butler('gen3-repo', writeable=True)
registry = butler.registry

default_collection = 'LSSTCam-imSim/defaults'

if default_collection in list(registry.queryCollections()):
    registry.removeCollection(default_collection)

registry.registerCollection(default_collection,
                            type=daf_butler.CollectionType.CHAINED)
registry.setCollectionChain(default_collection,
                            ['LSSTCam-imSim/raw/all', 'LSSTCam-imSim/calib',
                             'skymaps', 'ref_cat'])
