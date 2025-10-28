import pandas as pd
from dotenv import load_dotenv
from transforms.funcionario import transformar_dados_funcionario
from transforms.industria import transformar_dados_industria
from transforms.setor import transformar_dados_setor
from transforms.planos import transformar_dados_planos
from transforms.unidade import UnidadeRPA
from transforms.assinatura import transformar_dados_assinatura
from utils.db import conectar_banco

load_dotenv()

def main():

    #-----------------------------------#
    #           CONEXÃO PADRÃO          #
    #-----------------------------------#
    conn_origem = conectar_banco("primeiro")
    conn_destino = conectar_banco("segundo")

    #-----------------------------------#
    #   DF_ORIGEM (para cada tabela)    #
    #-----------------------------------#
    df_origem_funcionario = pd.read_sql(
        "SELECT id, nome, sobrenome, email, senha, cargo, id_setor FROM funcionario;", conn_origem
    )
    df_origem_industria = pd.read_sql(
        "SELECT id, nome, cnpj, email, dt_cadastro FROM empresa;", conn_origem
    )
    df_origem_setor = pd.read_sql(
        "SELECT id, nome, descricao, id_unidade FROM setor;", conn_origem
    )
    df_origem_plano = pd.read_sql(
        "SELECT id, nome, preco, tempo FROM plano;", conn_origem
    )
    df_origem_unidade = pd.read_sql(
        "SELECT id, cnpj, nome, email, endereco_cep, endereco_numero, endereco_complemento, id_empresa FROM unidade;", conn_origem
    )
    df_origem_assinatura = pd.read_sql(
        "SELECT id, id_empresa, id_plano, dt_inicio, dt_fim, status FROM assinatura;", conn_origem
    )

    #-----------------------------------#
    #   DF_DESTINO (modelo vazio)       #
    #-----------------------------------#
    df_destino_funcionario = pd.DataFrame(columns=['nome','sobrenome','email','senha','cargo','setor_id','unidade_id'])
    df_destino_industria = pd.DataFrame(columns=['id','nome','cnpj','email','data_cadastro'])
    df_destino_setor = pd.DataFrame(columns=['id','nome','descricao'])
    df_destino_plano = pd.DataFrame(columns=['id','nome','preco','duracao_meses'])
    df_destino_assinatura = pd.DataFrame(columns=['id','industria_id','plano_id','data_inicio','data_fim','status'])

    #-------------------------------------#
    # DF_TRANSFORMADO (para cada tabela)  #
    #-------------------------------------#
    df_transformado_funcionario = transformar_dados_funcionario(df_origem_funcionario, df_destino_funcionario, df_origem_setor)
    df_transformado_industria = transformar_dados_industria(df_origem_industria, df_destino_industria)
    df_transformado_setor = transformar_dados_setor(df_origem_setor, df_destino_setor)
    df_transformado_plano = transformar_dados_planos(df_origem_plano, df_destino_plano)

    # Usando a classe UnidadeRPA para transformar unidades
    unidade_rpa = UnidadeRPA()
    df_transformado_unidade = unidade_rpa.transformar(df_origem_unidade)

    df_transformado_assinatura = transformar_dados_assinatura(df_origem_assinatura, df_destino_assinatura)

    cursor = conn_destino.cursor()

    #-----------------------------------#
    # ORDEM CORRETA DE INSERÇÃO         #
    #-----------------------------------#

    # 1 - Plano
    try:
        insert_sql_plano = """
        INSERT INTO plano (id, nome, preco, duracao_meses)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET nome = EXCLUDED.nome,
            preco = EXCLUDED.preco,
            duracao_meses = EXCLUDED.duracao_meses;
        """
        for _, row in df_transformado_plano.iterrows():
            cursor.execute(insert_sql_plano, (row['id'], row['nome'], row['preco'], row['duracao_meses']))
        conn_destino.commit()
        print("Plano OK")
    except Exception as e:
        print("Erro ao inserir dados no banco de destino (Plano):", e)
        conn_destino.rollback()

    # 2 - Industria
    try:
        insert_sql_industria = """
        INSERT INTO industria (id, nome, cnpj, email, data_cadastro)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET nome = EXCLUDED.nome,
            cnpj = EXCLUDED.cnpj,
            email = EXCLUDED.email,
            data_cadastro = EXCLUDED.data_cadastro;
        """
        for _, row in df_transformado_industria.iterrows():
            cnpj_limpo = ''.join(filter(str.isdigit, row['cnpj']))[:14]
            cursor.execute(insert_sql_industria, (
                row['id'],
                row['nome'][:100],
                cnpj_limpo,
                row['email'][:100],
                pd.to_datetime(row['data_cadastro'])
            ))
        conn_destino.commit()
        print("Industria OK")
    except Exception as e:
        print("Erro ao inserir dados no banco de destino (Industria):", e)
        conn_destino.rollback()

    # 3 - Unidade
    try:
        insert_sql_unidade = """
        INSERT INTO unidade (
            id, nome, email, cep, rua, bairro, cidade, estado, numero, industria_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET nome = EXCLUDED.nome,
            email = EXCLUDED.email,
            cep = EXCLUDED.cep,
            rua = EXCLUDED.rua,
            bairro = EXCLUDED.bairro,
            cidade = EXCLUDED.cidade,
            estado = EXCLUDED.estado,
            numero = EXCLUDED.numero,
            industria_id = EXCLUDED.industria_id;
        """
        for _, row in df_transformado_unidade.iterrows():
            cursor.execute(insert_sql_unidade, (
                row['id'], row['nome'], row['email'], row['cep'], row['rua'],
                row['bairro'], row['cidade'], row['estado'], row['numero'], row['industria_id']
            ))
        conn_destino.commit()
        print("Unidades OK")
    except Exception as e:
        print("Erro ao inserir dados no banco de destino (Unidades):", e)
        conn_destino.rollback()

    # 4 - Setor
    try:
        insert_sql_setor = """
        INSERT INTO setor (id, nome, descricao)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET nome = EXCLUDED.nome,
            descricao = EXCLUDED.descricao;
        """
        for _, row in df_transformado_setor.iterrows():
            cursor.execute(insert_sql_setor, (row['id'], row['nome'][:100], row['descricao'][:250]))
        conn_destino.commit()
        print("Setor OK")
    except Exception as e:
        print("Erro ao inserir dados no banco de destino (Setor):", e)
        conn_destino.rollback()

    # 5 - Funcionário
    try:
        insert_sql_funcionario = """
        INSERT INTO funcionario (id, nome, sobrenome, email, senha, cargo, foto_perfil, setor_id, unidade_id)
        OVERRIDING SYSTEM VALUE
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET nome = EXCLUDED.nome,
            sobrenome = EXCLUDED.sobrenome,
            email = EXCLUDED.email,
            senha = EXCLUDED.senha,
            cargo = EXCLUDED.cargo,
            setor_id = EXCLUDED.setor_id,
            unidade_id = EXCLUDED.unidade_id;
        """

        for _, row in df_transformado_funcionario.iterrows():
            cursor.execute(insert_sql_funcionario, (
                row['id'],                # mantém o id original da origem
                row['nome'][:100],
                row['sobrenome'][:100],
                row['email'][:100],
                row['senha'][:260],
                row['cargo'],
                None,                     # foto_perfil não vem da origem
                row['setor_id'],
                row['unidade_id']
            ))

        conn_destino.commit()
        print("Funcionários OK ✅")
    except Exception as e:
        print("Erro ao inserir dados no banco de destino (Funcionario):", e)
        conn_destino.rollback()

    # 6 - Assinatura
    try:
        insert_sql_assinatura = """
        INSERT INTO assinatura (id, industria_id, plano_id, data_inicio, data_fim, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET industria_id = EXCLUDED.industria_id,
            plano_id = EXCLUDED.plano_id,
            data_inicio = EXCLUDED.data_inicio,
            data_fim = EXCLUDED.data_fim,
            status = EXCLUDED.status;
        """
        for _, row in df_transformado_assinatura.iterrows():
            cursor.execute(insert_sql_assinatura, (
                row['id'],
                row['industria_id'],
                row['plano_id'],
                pd.to_datetime(row['data_inicio']),
                pd.to_datetime(row['data_fim']),
                row['status']
            ))
        conn_destino.commit()
        print("Assinaturas OK")
    except Exception as e:
        print("Erro ao inserir dados no banco de destino (Assinaturas):", e)
        conn_destino.rollback()

    finally:
        cursor.close()
        conn_origem.close()
        conn_destino.close()


if __name__ == "__main__":
    main()
