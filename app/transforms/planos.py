import pandas as pd

def transformar_dados_planos(df_origem, df_destino):
    
    df_destino['id'] = df_origem['id'].astype(int)

    df_destino['nome'] = df_origem['nome'].astype(str).str.capitalize()

    df_destino['preco'] = df_origem['preco'].astype(float).round(2)

    # PERGUNTAR SOBRE DURAÇÃO_MESES
    # df_destino['duracao_meses'] = df_origem['duracao"].astype(int)

    return df_destino[['id', 'nome', 'preco']]
#   return df_destino[['id', 'nome', 'preco', 'duracao_meses]]


