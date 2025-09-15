import pandas as pd

def transformar_dados_industria(df_origem, df_destino) -> pd.DataFrame:
    

    df_destino['id'] = df_origem['id'].astype(int)

    df_destino['nome'] = df_origem['nome'].astype(str).str.capitalize()

    df_destino['cnpj'] = df_origem['cnpj'].astype(str).str.capitalize()

    df_destino['email'] = df_origem['email'].astype(str).str.capitalize()

    df_destino["data_cadastro"] = pd.to_datetime(df_origem["data_cadastro"], errors="coerce")

    return df_destino[['id','nome','cnpj','email','data_cadastro']]