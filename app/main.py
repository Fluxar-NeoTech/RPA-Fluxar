import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
import pandas as pd


load_dotenv()

def conectar_banco(serie):
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"), 
            dbname=os.getenv(f"DB_NAME_{serie.upper()}"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        print(f"Connection OK ")
        return conn
    except Exception as e:
        print("Erro ao conectar ao banco:", e)
        exit()

def consultar_dados(serie, query):
    # Abrindo a conexão com o banco de dados 
    conn = conectar_banco(serie)
    # 
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print("Erro ao consultar dados:", e)
        return None
    finally:   
        conn.close()

def transformar_dados_planos(df: pd.DataFrame) -> pd.DataFrame:

    df['nome'] = df['nome'].str.strip().str.title()
    df['preco'] = df['preco'].apply(lambda x: max(x, 0))
    df['duracao_meses'] = df['duracao']

    return df[['id', 'nome', 'preco', 'duracao_meses']]

def transformar_dados_industria(df: pd.DataFrame) -> pd.DataFrame:
    df['nome'] = df['nome'].str.strip().str.title().str[:100]
    df['email'] = df['email'].str.strip().str.lower().str[:100]
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro'], errors='coerce')

    df_destino = df[['id', 'nome', 'cnpj', 'email', 'data_cadastro']].drop_duplicates()

    return df_destino


def transformar_dados_funcionario(df: pd.DataFrame) -> pd.DataFrame:

    df['id'] = df['id'].astype(int)
    df['nome'] = df['nome'].str.strip().str.title().str[:100]
    df['sobrenome'] = df['sobrenome'].str.strip().str.title().str[:100]
    df['email'] = df['email'].str.strip().str.lower().str[:100]

    def mapear_cargo(valor):
        v = str(valor).upper()
        if "G" in v:
            return "G"
        elif "A" in v:
            return "A"
        return None

    df['cargo'] = df['cargo'].apply(mapear_cargo)
    df['senha'] = df['senha'].astype(str)

    return df[['id', 'nome', 'sobrenome', 'email', 'cargo', 'senha']]
def transformar_dados_unidade(df: pd.DataFrame) -> pd.DataFrame:

    df['nome'] = df['nome'].str.strip().str.title().str[:100]
    df['endereco'] = df['endereco'].fillna('').str.strip()

    df_destino = pd.DataFrame()
    df_destino['id'] = df['id']
    df_destino['nome'] = df['nome']
    df_destino['cep'] = df['cep']

    # Divide o campo endereco em palavras
    partes = df['endereco'].str.split(' ', expand=True)

    df_destino['rua'] = partes[0]  # primeira palavra
    df_destino['cidade'] = partes[1] if partes.shape[1] > 1 else None
    df_destino['estado'] = partes[2] if partes.shape[1] > 2 else None
    df_destino['bairro'] = partes[3] if partes.shape[1] > 3 else None
    df_destino['numero'] = df['numero']

    return df_destino

