import pandas

def transformar_dados_setor(df_origem, df_destino):
    df_destino['id'] = df_origem['id'].astype(int)

    df_destino['nome'] = df_origem['nome'].astype(str).str.capitalize()

    # Descrição do setor ainda precisa ser discutido
    # df_destinho['descricao'] = df_origem['descricao'].astype(str).str.capitalize()

    return df_destino[['id','nome']]

