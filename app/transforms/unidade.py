import pandas as pd

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