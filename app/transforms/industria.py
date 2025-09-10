import pandas as pd

def transformar_dados_industria(df: pd.DataFrame) -> pd.DataFrame:
    df['nome'] = df['nome'].str.strip().str.title().str[:100]
    df['email'] = df['email'].str.strip().str.lower().str[:100]
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro'], errors='coerce')

    df_destino = df[['id', 'nome', 'cnpj', 'email', 'data_cadastro']].drop_duplicates()

    return df_destino