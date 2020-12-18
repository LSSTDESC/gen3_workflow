DC2_filterMap = {f'{_}_sim_1.4': f'lsst_{_}_smeared' for _ in 'ugrizy'}
DC2_filterMap.update({_: f'lsst_{_}_smeared' for _ in 'ugrizy'})
config.astromRefObjLoader.filterMap = DC2_filterMap
config.photoRefObjLoader.filterMap = DC2_filterMap
