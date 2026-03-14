import pandas as pd
import fastf1
import logging
from pathlib import Path

#Configs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = Path(__file__).parent.parent
DIR_RAW = BASE_DIR / 'data' / 'raw'
DIR_PROCESSED = BASE_DIR / 'data' / 'processed'

YEAR = 2024
RACES = 24

fastf1.Cache.enable_cache(DIR_RAW)

#Utils

def time_to_str(time:str) -> str:

    """Remove os primeiros 6 caracteres do timedelta string"""

    return time[6:]

#Extract

def extract(num_race:int) -> pd.DataFrame:

    '''Carrega a sessão de classificação e retorna as voltas e o nome da corrida.'''

    logging.info(f'[EXTRACT] Etapa {num_race} - Carregando sessão...')

    session = fastf1.get_session(YEAR, num_race, 'Q')
    session.load()
    laps = session.laps
    race = session.event['EventName']
    laps['Race'] = race
    laps['RaceNumber'] = num_race

    logging.info(f'[EXTRACT] Etapa {num_race} - {race} carregado com sucesso.')

    return laps

#Transform

def get_perfect_lap(laps: pd.DataFrame, num_race: int) -> pd.DataFrame:

    '''Retorna um Dataframe com as voltas e os setores mais rápidos de cada piloto.
       Além disso, calcula a volta perfeita teórica'''

    logging.info(f'[TRANSFORM] Etapa {num_race} - Calculando a volta perfeita...')

    fastest_laps = (laps.groupby(by=['Driver'], as_index=False)
                    ['LapTime'].min()
                    .sort_values(by=['LapTime'])
                    )
    
    sectors = laps[['Driver', 'Sector1Time', 'Sector2Time', 'Sector3Time', 'Race', 'RaceNumber']]
    fastest_sectors = sectors.groupby(by=['Driver'], as_index=False).min()

    bestS1 = sectors['Sector1Time'].min()
    bestS2 = sectors['Sector2Time'].min()
    bestS3 = sectors['Sector3Time'].min()

    perfect_lap = bestS1 + bestS2 + bestS3

    fastest_laps_sectors = fastest_laps.merge(right=fastest_sectors,
                                              how='left',
                                              on='Driver')
    
    fastest_laps_sectors['IdealLap'] = perfect_lap

    logging.info(f'[TRANSFORM] Etapa {num_race} - Volta perfeita calculada...')

    return fastest_laps_sectors   

def diff_time(df: pd.DataFrame, num_race: int) -> pd.DataFrame:

    '''Calcula diferença da volta do piloto para a volta perfeita'''

    logging.info(f'[TRANSFORM] Etapa {num_race} - Calculando a diferença...')

    df['DiffLap'] = df['LapTime'] - df['IdealLap']

    logging.info(f'[TRANSFORM] Etapa {num_race} - Calculo da diferença finalizado.')

    return df

def diff_sec(df: pd.DataFrame, num_race: int) -> pd.DataFrame:

    '''Melhor Volta, Volta Perfeita, Diferença em segundos'''
    
    logging.info(f'[TRANSFORM] Etapa {num_race} - Criando colunas dos tempos em segundos...')

    df['LapTimeSec'] = df['LapTime'].dt.total_seconds()
    df['IdealLapSec'] = df['IdealLap'].dt.total_seconds()
    df['DiffLapSec'] = df['DiffLap'].dt.total_seconds()

    logging.info(f'[TRANSFORM] Etapa {num_race} - Colunas em segundos finalizadas.')

    return df

def fix_time(df: pd.DataFrame, num_race: int) -> pd.DataFrame:

    '''Aplica uma função para fatiar um datetime, deixando-o mais compreensível'''
    
    logging.info(f'[TRANSFORM] Etapa {num_race} - Organizando o datetime...')

    df['LapTime'] = df['LapTime'].astype(str).apply(time_to_str)
    df['Sector1Time'] = df['Sector1Time'].astype(str).apply(time_to_str)
    df['Sector2Time'] = df['Sector2Time'].astype(str).apply(time_to_str)
    df['Sector3Time'] = df['Sector3Time'].astype(str).apply(time_to_str)
    df['IdealLap'] = df['IdealLap'].astype(str).apply(time_to_str)
    df['DiffLap'] = df['DiffLap'].astype(str).apply(time_to_str)

    logging.info(f'[TRANSFORM] Etapa {num_race} - Datetime organizado.')

    return df

def organize_columns(df: pd.DataFrame, num_race: int) -> pd.DataFrame:

    '''Muda a ordem das colunas do DataFrame'''

    logging.info(f'[TRANSFORM] Etapa {num_race} - Organizando as colunas...')

    new_order = ['Driver', 'LapTime', 'Sector1Time', 'Sector2Time', 'Sector3Time', 'IdealLap', 'DiffLap', 'LapTimeSec', 'IdealLapSec', 'DiffLapSec', 'Race', 'RaceNumber']
    df = df[new_order]

    logging.info(f'[TRANSFORM] Etapa {num_race} - Colunas organizadas...')

    return df

#Load

def load_path(df: pd.DataFrame, num_race: int) -> None:

    '''Salva o DataFrame como CSV localmente.'''

    DIR_PROCESSED.mkdir(parents=True, exist_ok=True)
    file_name = f'comparacao_ano_{YEAR}_etapa_{num_race}'
    local_path = DIR_PROCESSED / file_name

    df.to_csv(local_path, index=False)
    logging.info(f'[LOAD] Etapa {num_race} - Arquivo salvo em: {local_path}')

#Pipeline

def data_transformation(num_race: int) -> None:

    '''Orquestra as etapas de Extract, Transform e Load.'''

    try:

        df = extract(num_race)
        df = get_perfect_lap(df, num_race)
        df = diff_time(df, num_race)
        df = diff_sec(df, num_race)
        df = fix_time(df, num_race)
        df = organize_columns(df, num_race)
        load_path(df, num_race)

    except Exception as error:

        logging.error(f'[PIPELINE] Erro na etapa {num_race}: {error}')

#Main

if __name__ == '__main__':

    logging.info(f'Iniciando pipeline - Temporada {YEAR}')

    for race in range(1, RACES + 1):
        data_transformation(race)

logging.info('Pipeline finalizado.')
