import pandas as pd
import requests
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("API_KEY_MAPS")

def pegar_endereco_por_cep(cep, api_key):
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={cep}&key={api_key}"
    response = requests.get(url)
    if response.status_code != 200:
        return {}
    dados = response.json()
    if dados['status'] != 'OK':
        return {}
    
    resultado = dados['results'][0]
    componentes = resultado['address_components']
    endereco = {}
    for comp in componentes:
        tipos = comp['types']
        if 'route' in tipos:
            endereco['rua'] = comp['long_name']
        elif 'sublocality_level_1' in tipos or 'neighborhood' in tipos:
            endereco['bairro'] = comp['long_name']
        elif 'locality' in tipos:
            endereco['cidade'] = comp['long_name']
        elif 'administrative_area_level_1' in tipos:
            endereco['estado'] = comp['short_name']
    return endereco

def transformar_dados_unidade(df_origem, df_endereco, df_destino) -> pd.DataFrame:
    # Inicializa colunas
    df_destino['id'] = df_origem['id'].astype(int)
    df_destino['nome'] = df_origem['nome'].astype(str).str.title()
    df_destino['cep'] = ''
    df_destino['rua'] = ''
    df_destino['bairro'] = ''
    df_destino['cidade'] = ''
    df_destino['estado'] = ''
    df_destino['numero'] = ''
    df_destino['industria_id'] = df_origem['empresa_id'].astype(int)

    # Preenche os dados de endereço
    for idx, row in df_origem.iterrows():
        endereco_row = df_endereco[df_endereco['id'] == row['id_endereco']].iloc[0]
        cep = endereco_row['cep']
        numero = str(endereco_row['numero'])
        endereco_completo = pegar_endereco_por_cep(cep, API_KEY)
        
        df_destino.at[idx, 'cep'] = cep
        df_destino.at[idx, 'numero'] = numero
        df_destino.at[idx, 'rua'] = endereco_completo.get('rua', '')
        df_destino.at[idx, 'bairro'] = endereco_completo.get('bairro', '')
        df_destino.at[idx, 'cidade'] = endereco_completo.get('cidade', '')
        df_destino.at[idx, 'estado'] = endereco_completo.get('estado', '')

    return df_destino[['id','nome','cep','rua','bairro','cidade','estado','numero','industria_id']]
