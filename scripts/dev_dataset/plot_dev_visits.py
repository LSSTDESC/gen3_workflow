import os
import glob
import pickle
import matplotlib.pyplot as plt
import lsst.daf.persistence as dp
from lsst.obs.lsst.imsim import ImsimMapper
import desc.sim_utils
from desc.sim_utils.sky_map_depth import get_obs_md_from_raw
plt.ion()

camera = ImsimMapper().camera
sensors = [_.getName() for _ in camera]

run20_region = desc.sim_utils.Run20Region()

ax = None
repo = 'repo'
butler = dp.Butler(repo)
colors = 'red green blue yellow cyan magenta'.split()
for band, color in zip('ugrizy', colors):
#for band, color in zip('u', colors):
    datarefs = butler.subset('raw', dataId={'filter': band, 'raftName': 'R22',
                                            'detectorName': 'S11'})
    visits = list(set(_.dataId['visit'] for _ in datarefs))
    for visit in visits:
        print(band, visit)
        raw_file = glob.glob(os.path.join(repo, 'raw', f'{visit}', 'R22',
                                          '*'))[0]
        obs_md = get_obs_md_from_raw(raw_file)
        ax = desc.sim_utils.plot_sensors(sensors, camera, obs_md,
                                         ax=ax, color=color, figsize=(18, 16))

    run20_region.plot_boundary()

skyMap_file = 'skyMap.pickle'
with open(skyMap_file, 'rb') as fd:
    skyMap = pickle.load(fd)

ax = desc.sim_utils.plot_skymap_tract(skyMap, tract=3828, ax=ax)

plt.xlabel('RA (deg)')
plt.ylabel('Dec (deg)')
plt.xlim(75, 48)
plt.ylim(-46, -26)
plt.title('Proposed Gen3 dev dataset from Run2.2i y01')
plt.savefig('gen3_dev_dataset.png')
