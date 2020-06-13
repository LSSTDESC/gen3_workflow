import sqlite3
import pandas as pd

with sqlite3.connect('tracts_mapping.sqlite3') as conn:
    df = pd.read_sql('select * from overlaps where visit <= 262078', conn)

df['patch'] = ['{},{}'.format(*eval(_)) for _ in df['patch']]

df.to_pickle('tracts_mapping_y01.pkl')
