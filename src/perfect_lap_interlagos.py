import pandas as pd

import fastf1
fastf1.Cache.enable_cache('../data/raw')
session = fastf1.get_session(2025, 'Interlagos', 'Qualifying')
session.load()
laps = session.laps

faster_lap = laps.pick_fastest()
faster_lap

faster_laps = laps.groupby(by=['Driver'])[['LapTime']].min().sort_values(by=['LapTime'])

sectors = laps[['Driver', 'Sector1Time', 'Sector2Time', 'Sector3Time']]

faster_sectors = (sectors.groupby(by=['Driver'], as_index=False).min())

bestS1 = faster_sectors['Sector1Time'].min()
bestS2 = faster_sectors['Sector2Time'].min()
bestS3 = faster_sectors['Sector3Time'].min()

ideal_lap = bestS1 + bestS2 + bestS3

def fix_time(x:str):
    return x[7:]

faster_laps.merge(right=faster_sectors,
                  how='left',
                  on='Driver',
                 )

faster_laps['IdealLap'] = ideal_lap

faster_laps['DiffLap'] = faster_laps['LapTime'] - faster_laps['IdealLap']

faster_laps['LapTimeSec'] = faster_laps['LapTime'].dt.total_seconds()
faster_laps['IdealLapSec'] = faster_laps['IdealLap'].dt.total_seconds()
faster_laps['DiffLapSec'] = faster_laps['DiffLap'].dt.total_seconds()

faster_laps['LapTime'] = faster_laps['LapTime'].astype(str).apply(fix_time)
faster_laps['IdealLap'] = faster_laps['IdealLap'].astype(str).apply(fix_time)
faster_laps['DiffLap'] = faster_laps['DiffLap'].astype(str).apply(fix_time)

df_final = faster_laps

df_final = df_final.sort_values(by=['DiffLapSec'])

df_final

df_final.to_csv('../data/processed/comparacao_volta_perfeita.csv', index=False)