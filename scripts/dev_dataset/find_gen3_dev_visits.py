import pickle
import sqlite3
import numpy as np
import pandas as pd
import lsst.geom
from lsst.sims.utils import angularSeparation

with open('skyMap.pickle', 'rb') as fd:
    sky_map = pickle.load(fd)
tract = 3828
tract_info = sky_map[tract]
center = tract_info.getCtrCoord()

ra0, dec0 = center.getLongitude().asDegrees(), center.getLatitude().asDegrees()

opsim_db_file = 'minion_1016_desc_dithered_v4_trimmed.db'

query = '''select obsHistID, descDitheredRA, descDitheredDec, expMJD,
           filter from Summary'''
with sqlite3.connect(opsim_db_file) as conn:
    df = pd.read_sql(query, conn)
df['ang_sep'] = angularSeparation(np.degrees(df['descDitheredRA'].to_numpy()),
                                  np.degrees(df['descDitheredDec'].to_numpy()),
                                  ra0, dec0)
y01 = min(df['expMJD']) + 365.

radius = 1.5
for band in 'ugrizy':
    my_df = df.query(f'expMJD < {y01} and filter == "{band}" '
                     f'and ang_sep < {radius}')
    for i in range(5):
        print(band, my_df.iloc[i].obsHistID)
    print()
