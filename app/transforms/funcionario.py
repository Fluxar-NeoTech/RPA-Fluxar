import pandas as pd

def transformar_dados_funcionario(df_origem, df_destino, df_setor):
    """
    Versão robusta e instrumentada da transformação de funcionários.
    Imprime checks e devolve df_destino pronto para inserção (mas sem inserir).
    """
    # ------------- checagens iniciais ----------------
    required_origem = {'id','nome','sobrenome','email','senha','cargo','id_setor'}
    missing = required_origem - set(df_origem.columns)
    if missing:
        raise KeyError(f"Colunas faltando em df_origem (funcionario): {missing}. Verifique o SELECT.")

    if 'id' not in df_setor.columns or 'id_unidade' not in df_setor.columns:
        raise KeyError("df_setor deve conter as colunas 'id' e 'id_unidade'.")

    # Cria df_destino limpo
    df_destino = pd.DataFrame()

    # Normaliza e garante tipos
    df_destino['id'] = pd.to_numeric(df_origem['id'], errors='coerce').astype('Int64')
    df_destino['nome'] = df_origem['nome'].astype(str).str.strip().str.title().str[:100]
    df_destino['sobrenome'] = df_origem['sobrenome'].astype(str).str.strip().str.title().str[:100]
    df_destino['email'] = df_origem['email'].astype(str).str.strip().str.lower().str[:100]
    df_destino['senha'] = df_origem['senha'].astype(str).str[:260]

    # Padroniza cargo
    cargos_g = ['gestor','g','gestor de estoque']
    cargos_a = ['analista','a','analista de sistemas']

    def corrigir_cargo(cargo):
        texto = str(cargo).strip().lower()
        if any(t == texto or t in texto for t in cargos_g):
            return 'G'
        if any(t == texto or t in texto for t in cargos_a):
            return 'A'
        return 'A'  # default

    df_destino['cargo'] = df_origem['cargo'].apply(corrigir_cargo)

    # foto_perfil vazia (nullable)
    df_destino['foto_perfil'] = None

    # setor_id
    df_destino['setor_id'] = pd.to_numeric(df_origem['id_setor'], errors='coerce').astype('Int64')

    # mapeia unidade_id via df_setor (set_index -> map)
    setor_map = df_setor.set_index('id')['id_unidade'].to_dict()
    df_destino['unidade_id'] = df_destino['setor_id'].map(lambda x: setor_map.get(int(x)) if pd.notna(x) else pd.NA).astype('Int64')

    # ------------- diagnósticos impressos --------------
    print("=== Diagnóstico funcionário ===")
    print("linhas originais:", len(df_origem))
    print("colunas origem:", list(df_origem.columns))
    print("colunas destino (pré-check):", list(df_destino.columns))
    print("ids nulos em destino['id']:", df_destino['id'].isna().sum())
    print("setor_id nulos:", df_destino['setor_id'].isna().sum())
    print("unidade_id nulos (após map):", df_destino['unidade_id'].isna().sum())

    # Mostra primeiras linhas com problemas para te ajudar a debugar
    if df_destino['id'].isna().any():
        print("Exemplos de linhas com id inválido:")
        print(df_origem[df_destino['id'].isna()].head(5))

    if df_destino['unidade_id'].isna().any():
        print("Exemplos de setores que não mapearam para unidade_id (possíveis setors faltando):")
        bad = df_destino[df_destino['unidade_id'].isna()].head(10)
        print(bad[['setor_id']].drop_duplicates())

    # ------------- Ações sugeridas automáticas -----------
    # Se quiser forçar remoção de linhas críticas com id ou setor missing:
    problema_critico = df_destino['id'].isna() | df_destino['setor_id'].isna()
    if problema_critico.any():
        print(f"Atenção: {problema_critico.sum()} linhas com 'id' ou 'setor_id' inválidos. Elas serão descartadas.")
        df_destino = df_destino[~problema_critico].copy()

    # Converte Int64 para int padrão antes de inserir, se preferir:
    # (CUIDADO: pegar apenas quando não há valores faltantes)
    if df_destino['id'].isna().sum() == 0:
        df_destino['id'] = df_destino['id'].astype(int)
    if df_destino['setor_id'].isna().sum() == 0:
        df_destino['setor_id'] = df_destino['setor_id'].astype(int)
    # unidade_id pode ser nulo; se for necessário para FK, trate antes do insert
    # aqui tentamos converter quando não há NA
    if df_destino['unidade_id'].isna().sum() == 0:
        df_destino['unidade_id'] = df_destino['unidade_id'].astype(int)

    # ------------- retorno -------------
    return df_destino[['id','nome','sobrenome','email','senha','cargo','foto_perfil','setor_id','unidade_id']]
