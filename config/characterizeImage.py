config.measurePsf.starSelector["objectSize"].doFluxLimit = False
config.measurePsf.starSelector["objectSize"].doSignalToNoiseLimit = True
config.measurePsf.starSelector["objectSize"].signalToNoiseMin = 20

# Discussed on Slack #desc-dm-dc2 with Lauren and set to 200 for Run2.1i
# For Run2.2i, setting to zero per
# https://github.com/LSSTDESC/ImageProcessingPipelines/issues/136
config.measurePsf.starSelector["objectSize"].signalToNoiseMax = 0

# S/N cuts for computing aperture corrections.
config.measureApCorr.sourceSelector['science'].doFlags = True
config.measureApCorr.sourceSelector['science'].doSignalToNoise = True
config.measureApCorr.sourceSelector['science'].flags.good = ['calib_psf_used']
config.measureApCorr.sourceSelector['science'].flags.bad = []
config.measureApCorr.sourceSelector['science'].signalToNoise.minimum = 150.0
config.measureApCorr.sourceSelector['science'].signalToNoise.maximum = None
config.measureApCorr.sourceSelector['science'].signalToNoise.fluxField = 'base_PsfFlux_instFlux'
config.measureApCorr.sourceSelector['science'].signalToNoise.errField = 'base_PsfFlux_instFluxErr'
config.measureApCorr.sourceSelector.name = 'science'
