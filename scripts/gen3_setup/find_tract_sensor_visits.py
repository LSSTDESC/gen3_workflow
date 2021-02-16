"""
Find raw files for a tract and subset of patches using the Run2.2i DR6
tracts mapping database.
"""
import os
import sys
import glob
import sqlite3
import pandas as pd
from lsst.obs.lsst import LsstCamMapper

camera = LsstCamMapper().camera
det_names = {_.getId(): _.getName() for _ in camera}

tract = 4849
patches = ['(0, 0)', '(0, 1)', '(0, 2)', '(0, 3)', '(0, 4)', '(0, 5)', '(0, 6)']
patch_list = "(" + ','.join([f'"{_}"' for _ in patches]) + ")"

#tract = 3828
#patch_list = None

visit_max = 262622

tracts_mapping_db = ('/global/cfs/cdirs/lsst/shared/DC2-prod/Run2.2i/'
                     'desc_dm_drp/v19.0.0/rerun/run2.2i-calexp-v1/'
                     'tracts_mapping.sqlite3')

assert(os.path.isfile(tracts_mapping_db))
with sqlite3.connect(tracts_mapping_db) as conn:
    query = ('select distinct visit, detector, filter, patch from overlaps '
             f'where tract={tract} and visit <= {visit_max}')
    if patch_list is not None:
        query += f' and patch in {patch_list}'
    df = pd.read_sql(query, conn)

missing_sensor_visits = []
raw_files = []
raw_image_root_dir = '/global/cfs/cdirs/lsst/shared/DC2-prod/Run2.2i/sim'
for iloc in range(len(df)):
    if iloc % (len(df)//20) == 0:
        sys.stdout.write('.')
        sys.stdout.flush()
    row = df.iloc[iloc]
    det_name = det_names[row.detector]
    pattern = os.path.join(raw_image_root_dir, 'y?-wfd', f'{row.visit:08d}',
                           f'lsst_a_{row.visit}_{det_name}_?.fits')
    try:
        raw_files.append(glob.glob(pattern)[0])
    except IndexError:
        missing_sensor_visits.append((visit, detector))
