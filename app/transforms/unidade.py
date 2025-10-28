import pandas as pd
import requests
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("API_KEY_MAPS")

class UnidadeRPA:
    """Classe para transformar dados de unidades do banco de origem para o padrão do banco de destino."""

    SIGLAS_VALIDAS = ['AC','AL','AP','AM','BA','CE','ES','DF','MA','MT','MS','MG','PA','PB',
                      'PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO','GO']

    def __init__(self, api_key: str = API_KEY):
        self.api_key = api_key
        self.cep_cache = {}

    def pegar_endereco_por_cep(self, cep: str) -> dict:
        """Consulta a API do Google Maps e retorna o endereço com ruas, bairro, cidade e estado."""
        cep = str(cep).replace('-', '')[:8]

        # Verifica cache
        if cep in self.cep_cache:
            return self.cep_cache[cep]

        url = f"https://maps.googleapis.com/maps/api/geocode/json?address={cep}&key={self.api_key}"
        response = requests.get(url)
        endereco = {}

        if response.status_code == 200:
            dados = response.json()
            if dados.get('status') == 'OK' and dados.get('results'):
                resultado = dados['results'][0]
                for comp in resultado.get('address_components', []):
                    tipos = comp.get('types', [])
                    if 'route' in tipos:
                        endereco['rua'] = comp.get('long_name', '')
                    elif 'sublocality_level_1' in tipos or 'neighborhood' in tipos:
                        endereco['bairro'] = comp.get('long_name', '')
                    elif 'locality' in tipos:
                        endereco['cidade'] = comp.get('long_name', '')
                    elif 'administrative_area_level_1' in tipos:
                        endereco['estado'] = comp.get('short_name', '').upper()

        # Valores padrão se faltar informação
        endereco.setdefault('rua', 'Rua não informada')
        endereco.setdefault('bairro', 'Bairro não informado')
        endereco.setdefault('cidade', 'Cidade não informada')
        estado = endereco.get('estado', 'SP').upper()
        endereco['estado'] = estado if estado in self.SIGLAS_VALIDAS else 'SP'

        # Salva no cache
        self.cep_cache[cep] = endereco
        return endereco

    def transformar(self, df_origem: pd.DataFrame) -> pd.DataFrame:
        """Transforma o dataframe de unidades do novo banco de origem para o padrão do banco de destino."""
        df_destino = pd.DataFrame()
        df_destino['id'] = df_origem['id'].astype(int)
        df_destino['nome'] = df_origem['nome'].astype(str).str.title().str[:50]
        df_destino['email'] = df_origem['email'].astype(str).str.lower().str[:50]
        df_destino['industria_id'] = df_origem['id_empresa'].astype(int)

        # Inicializa colunas de endereço
        for col in ['cep','rua','bairro','cidade','estado','numero']:
            df_destino[col] = ''

        for idx, row in df_origem.iterrows():
            cep = str(row['endereco_cep']).replace('-', '')[:8]
            numero = str(row['endereco_numero'])
            endereco_api = self.pegar_endereco_por_cep(cep)

            df_destino.at[idx, 'cep'] = cep
            df_destino.at[idx, 'numero'] = numero
            df_destino.at[idx, 'rua'] = endereco_api.get('rua')[:50]
            df_destino.at[idx, 'bairro'] = endereco_api.get('bairro')[:50]
            df_destino.at[idx, 'cidade'] = endereco_api.get('cidade')[:50]
            df_destino.at[idx, 'estado'] = endereco_api.get('estado')

        # Reordena colunas para coincidir com o destino
        return df_destino[['id','nome','email','cep','rua','bairro','cidade','estado','numero','industria_id']]
