import pandas as pd

def transformar_dados_funcionario(df_origem, df_destino):
    "Essa função irá receber o dataframe do banco de dados do primeiro e transformar os dados para o banco de dados do segundo"

    #Tranformações dos dados
    df_destino['id'] = df_origem['id'].astype(int)
    
    df_destino['nome'] = df_origem['nome'].astype(str).str.capitalize()

    df_destino['sobrenome'] = df_origem['sobrenome'].astype(str).str.capitalize()

    df_destino['email'] = df_origem['email'].astype(str).str.lower()

    df_destino['senha'] = df_origem['senha'].astype(str).str.lower()

    cargos_g = [
        'Gestor', 'gestor', 'GESTOR', 'g', 'G', 'Gestor de Estoque', 'gestor de estoque', 'GESTOR DE ESTOQUE'
    ]

    cargos_a = [
        'a', 'A', 'Analista', 'analista', 'ANALISTA', 'Analista de Sistemas', 'analista de sistemas', 'ANALISTA DE SISTEMAS'
    ]

    def corrigir_cargo(cargo):
        """Converte qualquer variação em 'G' ou 'A'"""
        texto = str(cargo).strip().lower()

        # procura o texto em cada item das listas
        if any(texto == item or texto in item for item in cargos_g):
            return "G"

        if any(texto == item or texto in item for item in cargos_a):
            return "A"

    df_destino['cargo'] = df_origem['cargo'].apply(corrigir_cargo)

    df_destino['setor_id'] = df_origem['id_setor'].astype(int)

    df_destino['unidade_id'] = df_origem['id_unidade'].astype(int)
    
    #Retornar o df que vai para o segundo 
    return df_destino[['id', 'nome', 'sobrenome', 'email', 'senha', 'cargo', 'setor_id', 'unidade_id']]

    # Ver senha e unidade_id
