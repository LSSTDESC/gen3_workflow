"""
Script to reformat the tracts_mapping.sqlite3 file into a pandas dataframe
where patch values are formatted more conveniently, e.g., '0,0' instead of
'(0, 0)'.  Entries are limited to y01 visits for DR2 processing.
"""
import os
import sqlite3
import pandas as pd

parent_repo = ('/global/cfs/cdirs/lsst/production/DC2_ImSim/Run2.2i'
               '/desc_dm_drp/v19.0.0-v1/rerun/run2.2i-calexp-v1')
tracts_mapping_db = os.path.join(parent_repo, 'tracts_mapping.sqlite3')

with sqlite3.connect(tracts_mapping_db) as conn:
    df = pd.read_sql('select * from overlaps where visit <= 262078', conn)

df['patch'] = ['{},{}'.format(*eval(_)) for _ in df['patch']]

df.to_pickle('tracts_mapping_y01.pkl')
