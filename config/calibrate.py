DC2_filterMap = {f'{_}_sim_1.4': f'lsst_{_}_smeared' for _ in 'ugrizy'}
DC2_filterMap.update({_: f'lsst_{_}_smeared' for _ in 'ugrizy'})
config.astromRefObjLoader.filterMap = DC2_filterMap
config.photoRefObjLoader.filterMap = DC2_filterMap

# Additional configs for star+galaxy ref cats now that DM-17917 is merged
config.astrometry.referenceSelector.doUnresolved = True
config.astrometry.referenceSelector.unresolved.name = 'resolved'
config.astrometry.referenceSelector.unresolved.minimum = None
config.astrometry.referenceSelector.unresolved.maximum = 0.5

# Make sure galaxies are not used for zero-point calculation.
config.photoCal.match.referenceSelection.doUnresolved = True
config.photoCal.match.referenceSelection.unresolved.name = 'resolved'
config.photoCal.match.referenceSelection.unresolved.minimum = None
config.photoCal.match.referenceSelection.unresolved.maximum = 0.5

# S/N cuts for zero-point calculation
config.photoCal.match.sourceSelection.doSignalToNoise = True
config.photoCal.match.sourceSelection.signalToNoise.minimum = 150
config.photoCal.match.sourceSelection.signalToNoise.fluxField = 'base_PsfFlux_instFlux'
config.photoCal.match.sourceSelection.signalToNoise.errField = 'base_PsfFlux_instFluxErr'
