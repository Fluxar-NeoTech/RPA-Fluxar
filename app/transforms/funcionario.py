import pandas as pd

def transformar_dados_funcionario(df_origem, df_destino):
    "Essa função irá receber o dataframe do banco de dados do primeiro e transformar os dados para o banco de dados do segundo"

    cargos_variacoes = ['Gestor', 'gestor', 'GESTOR', 'g', 'G', 'Gestor de Estoque', 'gestor de estoque', 'GESTOR DE ESTOQUE', 'a', 'A', 'Analista', 'analista', 'ANALISTA', 'Analista de Sistemas', 'analista de sistemas', 'ANALISTA DE SISTEMAS']

    #Tranformações dos dados
    df_destino['nome'] = df_origem['nome'].astype(str).str.capitalize()

    df_destino['sobrenome'] = df_origem['sobrenome'].astype(str).str.capitalize()

    df_destino['email'] = df_origem['email'].astype(str).str.lower()

    df_destino['senha'] = df_origem['senha'].astype(str).str.lower()

    def corrigir_cargo(cargo):
        if cargo in cargos_variacoes[:7]:
            return 'G'
        elif cargo in cargos_variacoes[7:]:
            return 'A'
        else:
            return None

    df_destino['cargo'] = df_origem['cargo'].apply(corrigir_cargo)

    df_destino['setor_id'] = df_origem['setor_id'].astype(int)

    df_destino['unidade_id'] = df_origem['unidade_id'].astype(int)
    
    #Retornar o df que vai para o segundo 
    return df_destino[['id', 'nome', 'sobrenome', 'email', 'senha', 'cargo', 'setor_id', 'unidade_id']]

