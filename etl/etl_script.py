import pandas as pd
import requests
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine

def extract(URL, area, inicio, fim, endpoint="cargaverificada"):    
    data_inicio = datetime.strptime(inicio, '%Y-%m-%d')
    data_fim = datetime.strptime(fim, '%Y-%m-%d')
    
    dataframes = [] 
    
    inicio_atual = data_inicio
    while inicio_atual <= data_fim:
        fim_atual = min(inicio_atual + pd.Timedelta(days=90), data_fim)

        params = {
            'cod_areacarga': area,
            'dat_inicio': inicio_atual.strftime('%Y-%m-%d'),
            'dat_fim': fim_atual.strftime('%Y-%m-%d')
        }

        url = f"{URL}/{endpoint}"
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            dados = response.json()
            if dados: 
                df = pd.DataFrame(dados)
                dataframes.append(df)
            else:
                print(f"Aviso: Dados não encontrados para o período {params['dat_inicio']} a {params['dat_fim']}.")
        else:
            print(f"Erro na requisição para o período {params['dat_inicio']} a {params['dat_fim']}: {response.status_code} - {response.text}")

        inicio_atual = fim_atual + pd.Timedelta(days=1)

    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)
        final_df.drop_duplicates(inplace=True) 
        return final_df
    else:
        return pd.DataFrame()


def transform(df):
    df_transformed = df.copy()
    df_transformed['din_referenciautc'] = pd.to_datetime(df_transformed['din_referenciautc']).dt.tz_localize(None) - pd.Timedelta(hours=3)

    df_transformed.rename(columns={'din_referenciautc': 'datetime'}, inplace=True)

    df_transformed = df_transformed[[
        'datetime', 'cod_areacarga', 'val_cargaglobal',
        'val_cargaglobalcons', 'val_cargaglobalsmmgd',
        'val_cargasupervisionada', 'val_carganaosupervisionada',
        'val_cargammgd', 'val_consistencia'
    ]]

    return df_transformed

def load(df, engine, table_name):
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)


if __name__ == "__main__":
    DB_NAME = os.getenv('POSTGRES_DB')
    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    TABLE_NAME = os.getenv('TABLE_NAME')
    API_URL = os.getenv('API_URL')
    AREA = os.getenv('AREA', 'NE')
    START_DATE_STR = os.getenv('START_DATE', '2022-01-01')
    END_DATE_STR = os.getenv('END_DATE', datetime.today().strftime('%Y-%m-%d'))

    # Validate area code
    ALLOWED_AREAS = [
        "SECO", "S", "NE", "N", "RJ", "SP", "MG", "ES", "MT", "MS", "DF", "GO",
        "AC", "RO", "PR", "SC", "RS", "BASE", "BAOE", "ALPE", "PBRN", "CE",
        "PI", "TON", "PA", "MA", "AP", "AM", "RR", "PESE", "PES", "PENE", "PEN"
    ]

    if AREA not in ALLOWED_AREAS:
        print(f"Erro: Código de área inválido '{AREA}'. Os códigos permitidos são: {', '.join(ALLOWED_AREAS)}", file=sys.stderr)
        sys.exit(1)

    # Validate date range
    MIN_ALLOWED_DATE = datetime(2016, 1, 1).date()
    MAX_ALLOWED_DATE = datetime.today().date()

    try:
        start_date = datetime.strptime(START_DATE_STR, '%Y-%m-%d').date()
        end_date = datetime.strptime(END_DATE_STR, '%Y-%m-%d').date()
    except ValueError:
        print("Erro: Formato de data inválido. Use YYYY-MM-DD.", file=sys.stderr)
        sys.exit(1)

    if start_date < MIN_ALLOWED_DATE:
        print(f"Erro: A data de início ({START_DATE_STR}) deve ser maior ou igual a {MIN_ALLOWED_DATE.strftime('%Y-%m-%d')}.", file=sys.stderr)
        sys.exit(1)

    if end_date > MAX_ALLOWED_DATE:
        print(f"Erro: A data final ({END_DATE_STR}) deve ser menor ou igual à data de hoje ({MAX_ALLOWED_DATE.strftime('%Y-%m-%d')}).", file=sys.stderr)
        sys.exit(1)

    if start_date > end_date:
        print(f"Erro: A data de início ({START_DATE_STR}) não pode ser maior que a data final ({END_DATE_STR}).", file=sys.stderr)
        sys.exit(1)

    df = extract(URL=API_URL, inicio=START_DATE_STR, fim=END_DATE_STR, area=AREA)

    if not df.empty:
        df_transformed = transform(df)

        try:
            engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
            load(df_transformed, engine, table_name=TABLE_NAME)
        except Exception as e:
            print(f"Erro ao conectar ou carregar dados no banco de dados: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("Nenhum dado foi extraído, pulando as etapas de transformação e carregamento.")