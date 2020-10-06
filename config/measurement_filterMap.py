DC2_filterMap = {f'{_}_sim_1.4': f'lsst_{_}_smeared' for _ in 'ugrizy'}
config.match.refObjLoader.filterMap = DC2_filterMap
