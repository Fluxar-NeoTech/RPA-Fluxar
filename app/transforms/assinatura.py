import pandas as pd

def transformar_dados_assinatura(df_origem, df_destino) -> pd.DataFrame:
    # Copiando e convertendo colunas
    df_destino['id'] = df_origem['id'].astype(int)
    df_destino['industria_id'] = df_origem['id_empresa'].astype(int)
    df_destino['plano_id'] = df_origem['id_plano'].astype(int)
    
    # Converte para datetime
    df_destino['data_inicio'] = pd.to_datetime(df_origem['dt_inicio'], errors='coerce')
    df_destino['data_fim'] = pd.to_datetime(df_origem['dt_fim'], errors='coerce')
    
    df_destino['status'] = df_origem['status'].astype(str)
    
    return df_destino[['id','industria_id','plano_id','data_inicio','data_fim','status']]
