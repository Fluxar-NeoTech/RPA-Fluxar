import pandas as pd

def transformar_dados_planos(df: pd.DataFrame) -> pd.DataFrame:

    df['nome'] = df['nome'].str.strip().str.title()
    df['preco'] = df['preco'].apply(lambda x: max(x, 0))
    df['duracao_meses'] = df['duracao']

    return df[['id', 'nome', 'preco', 'duracao_meses']]

