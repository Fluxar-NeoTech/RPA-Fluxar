import pandas as pd


def transformar_dados_funcionario(df_origem: pd.DataFrame, df_destino: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma os dados de funcionários do dataframe de origem para o formato do dataframe de destino.

    Args:
        df_origem (pd.DataFrame): DataFrame com os dados originais do banco de dados de origem.
        df_destino (pd.DataFrame): DataFrame modelo para receber os dados transformados.

    Returns:
        pd.DataFrame: DataFrame com os dados transformados prontos para inserção no destino.
    """

    # Transformações de dados
    df_destino["id"] = df_origem["id"].astype(int)

    df_destino["nome"] = df_origem["nome"].astype(str).str.capitalize()
    df_destino["sobrenome"] = df_origem["sobrenome"].astype(str).str.capitalize()

    df_destino["email"] = df_origem["email"].astype(str).str.lower()
    df_destino["senha"] = df_origem["senha"].astype(str).str.lower()

    cargos_g = [
        "gestor", "g", "gestor de estoque"
    ]

    cargos_a = [
        "analista", "a", "analista de sistemas"
    ]

    def corrigir_cargo(cargo: str) -> str:
        """Converte qualquer variação de cargo para 'G' (gestor) ou 'A' (analista)."""
        texto = str(cargo).strip().lower()

        if any(texto == item or texto in item for item in cargos_g):
            return "G"
        if any(texto == item or texto in item for item in cargos_a):
            return "A"
        return None

    df_destino["cargo"] = df_origem["cargo"].apply(corrigir_cargo)

    df_destino["setor_id"] = df_origem["id_setor"].astype(int)
    df_destino["unidade_id"] = df_origem["id_unidade"].astype(int)

    # Retornar dataframe pronto para o banco de destino
    return df_destino[
        ["id", "nome", "sobrenome", "email", "senha", "cargo", "setor_id", "unidade_id"]
    ]
