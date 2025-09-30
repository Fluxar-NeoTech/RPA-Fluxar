import pandas

def transformar_dados_setor(df: pd.DataFrame) -> pd.DataFrame:
    df['id'] = df['id'].astype(int)
    