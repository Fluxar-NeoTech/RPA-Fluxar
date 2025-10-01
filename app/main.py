import pandas as pd
from dotenv import load_dotenv

from transforms.funcionario import transformar_dados_funcionario
from transforms.industria import transformar_dados_industria
from transforms.setor import transformar_dados_setor
from transforms.planos import transformar_dados_planos
from transforms.unidade import transformar_dados_unidade
from transforms.assinatura import transformar_dados_assinatura
from utils.db import conectar_banco

load_dotenv()


def inserir_dados(cursor, query, dados, mensagem_ok: str, mensagem_erro: str, conn):
    """
    Executa inserções no banco de destino com tratamento de erros.

    Args:
        cursor: cursor do banco de destino
        query (str): comando SQL de inserção
        dados (iterable): dados iterados para inserção
        mensagem_ok (str): mensagem de sucesso
        mensagem_erro (str): mensagem de erro
        conn: conexão com o banco de destino
    """
    try:
        for row in dados:
            cursor.execute(query, row)
        conn.commit()
        print(mensagem_ok)
    except Exception as e:
        print(f"{mensagem_erro}: {e}")
        conn.rollback()


def main():
    # -----------------------------------#
    #           CONEXÕES                 #
    # -----------------------------------#
    conn_origem = conectar_banco("primeiro")
    conn_destino = conectar_banco("segundo")
    cursor = conn_destino.cursor()

    # -----------------------------------#
    #   DF_ORIGEM (dados brutos)         #
    # -----------------------------------#
    df_origem_funcionario = pd.read_sql(
        """
        SELECT id, nome, sobrenome, email, senha, cargo, id_setor, id_unidade
        FROM funcionario;
        """, conn_origem
    )
    df_origem_industria = pd.read_sql(
        "SELECT id, nome, cnpj, email, data_cadastro FROM empresa;", conn_origem
    )
    df_origem_setor = pd.read_sql(
        "SELECT id, nome, descricao FROM setor;", conn_origem
    )
    df_origem_plano = pd.read_sql(
        "SELECT id, nome, preco, tempo FROM plano;", conn_origem
    )
    df_origem_unidade = pd.read_sql(
        "SELECT id, nome, empresa_id, id_endereco FROM unidade;", conn_origem
    )
    df_origem_endereco = pd.read_sql(
        "SELECT id, cep, numero, complemento FROM endereco;", conn_origem
    )
    df_origem_assinatura = pd.read_sql(
        "SELECT id, id_empresa, id_plano, dt_inicio, dt_fim, status FROM assinatura;", conn_origem
    )

    # -----------------------------------#
    #   DF_DESTINO (modelos)             #
    # -----------------------------------#
    df_destino_funcionario = pd.DataFrame(
        columns=["id", "nome", "sobrenome", "email", "senha", "cargo", "setor_id", "unidade_id"]
    )
    df_destino_industria = pd.DataFrame(columns=["id", "nome", "cnpj", "email", "data_cadastro"])
    df_destino_setor = pd.DataFrame(columns=["id", "nome", "descricao"])
    df_destino_plano = pd.DataFrame(columns=["id", "nome", "preco", "duracao_meses"])
    df_destino_unidade = pd.DataFrame(
        columns=["id", "nome", "cep", "rua", "bairro", "cidade", "estado", "numero", "industria_id"])