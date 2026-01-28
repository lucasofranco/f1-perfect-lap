import pandas as pd
import fastf1

fastf1.Cache.enable_cache('../data/raw')

def time_to_str(time:str):
        return time[6:]


def processar_etapa(numero_corrida):

    try:

        print(f'Processando Etapa {numero_corrida}. de 2025')
                
        sessao = fastf1.get_session(2025, numero_corrida, 'Q')
        sessao.load()
        voltas = sessao.laps

        voltas_mais_rapidas = (voltas.groupby(by=['Driver'], as_index=False)['LapTime']
                                     .min()
                                     .sort_values(by=['LapTime'])
                              )

        setores = voltas[['Driver', 'Sector1Time', 'Sector2Time', 'Sector3Time',]]

        setores_mais_rapidos = setores.groupby(by=['Driver'], as_index=False).min()

        melhorS1 = setores_mais_rapidos['Sector1Time'].min()
        melhorS2 = setores_mais_rapidos['Sector2Time'].min()
        melhorS3 = setores_mais_rapidos['Sector3Time'].min()
                
        volta_mais_rapida = voltas_mais_rapidas['LapTime'].min()

        volta_perfeita = melhorS1 + melhorS2 + melhorS3

        dt_final = voltas_mais_rapidas

        dt_final['PerfeitaVolta'] = volta_perfeita
        dt_final['DiffVolta'] = dt_final['LapTime'] - dt_final['PerfeitaVolta']

        dt_final['TempoVoltaSec'] = dt_final['LapTime'].dt.total_seconds()
        dt_final['DiffVoltaSec'] = dt_final['DiffVolta'].dt.total_seconds()
        dt_final['PerfeitaVoltaSec'] = dt_final['PerfeitaVolta'].dt.total_seconds()

        dt_final['LapTime'] = dt_final['LapTime'].astype(str).apply(time_to_str)
        dt_final['PerfeitaVolta'] = dt_final['PerfeitaVolta'].astype(str).apply(time_to_str)
        dt_final['DiffVolta'] = dt_final['PerfeitaVolta'].astype(str).apply(time_to_str)

        colunas_renomeadas = {'Driver': 'Piloto',
                              'LapTime': 'TempoVolta',
                             }
                
        dt_final.rename(columns=colunas_renomeadas, inplace=True)

        nomeGP = sessao.event['EventName']
                
        dt_final['NumGP'] = numero_corrida
        dt_final['NomeGP'] = nomeGP

        filename = f'comparacao_2025_etapa_{numero_corrida}.csv'
        local_path = f'../data/processed/{filename}'
        dt_final.to_csv(local_path, index=False)
        
    except Exception as erro:

        print(f'Erro na etapa {numero_corrida}: {erro}')


print('Iniciando Processamento: Temporada 2025')

for etapa in range(1, 25):
    processar_etapa(etapa)

print('Processamento Finalizado')

df = pd.read_csv('../data/processed/comparacao_2025_etapa_21.csv')
df
