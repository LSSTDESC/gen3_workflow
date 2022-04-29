"""
Example script to generate a catalog of fake objects lying in a DC2 tract
for injection into a visit-level or coadd image.

This code steals/borrows from
https://github.com/LSSTDESC/ssi-tools/blob/main/examples/cosmoDC2_galaxy_hexgrid_example.ipynb
"""
import fitsio
import numpy as np
import pandas as pd
from ssi_tools.layout_utils import make_hexgrid_for_tract
import lsst.daf.butler as daf_butler

# DC2 Run2.2i repo at NERSC
repo = '/global/cfs/cdirs/lsst/production/gen3/DC2/Run2.2i/repo'
collections = ['LSSTCam-imSim/defaults']
butler = daf_butler.Butler(repo, collections=collections)

tract_id = 3828
skymap = butler.get('skyMap', name='DC2')
tract = skymap[tract_id]

# The cosmoDC2 fake galaxy catalog is available at NERSC in
# /global/cfs/cdirs/lsst/groups/fake-source-injection/DC2/catalogs/
srcs0 = fitsio.read('cosmoDC2_v1.1.4_small_fsi_catalog.fits')
grid = make_hexgrid_for_tract(tract, rng=100)
rng = np.random.RandomState(seed=42)
indexes = rng.choice(len(srcs0), size=len(grid), replace=True)

srcs = srcs0[indexes].copy()
srcs['raJ2000'] = np.deg2rad(grid['ra'])
srcs['decJ2000'] = np.deg2rad(grid['dec'])

mask = tract.getBBox().contains(grid['x'], grid['y'])
srcs = srcs[mask].copy()

data = {name: srcs[name].tolist() for name in srcs.dtype.names}
df = pd.DataFrame(data=data)
df.to_parquet(f'ssi_catalog_{tract_id}.parq')
