import pandas as pd
from dotenv import load_dotenv
from transforms.funcionario import transformar_dados_funcionario
from transforms.industria import transformar_dados_industria
from transforms.setor import transformar_dados_setor
from transforms.planos import transformar_dados_planos
from utils.db import conectar_banco
from transforms.unidade import transformar_dados_unidade
from transforms.assinatura import transformar_dados_assinatura

load_dotenv()

def main():

    #-----------------------------------#
    #           CONEXÃO PADRÃO          #
    #-----------------------------------#
    conn_origem = conectar_banco("primeiro")
    conn_destino = conectar_banco("segundo")

    #-----------------------------------#
    #   DF_ORIGEM ( para cada tabela)   #
    #-----------------------------------#
    query_funcionario = "SELECT id, nome, sobrenome, email, senha, cargo, setor_id, unidade_id FROM funcionario;"
    df_origem_funcionario = pd.read_sql(query_funcionario, conn_origem)

    query_industria = "SELECT id, nome, cnpj, email, data_cadastro FROM industria;"
    df_origem_industria = pd.read_sql(query_industria, conn_origem)

    query_setor = "SELECT id,nome FROM setor;"
    df_origem_setor = pd.read_sql(query_setor, conn_origem)

    query_plano = "SELECT id,nome,preco FROM plano;"
    df_origem_plano = pd.read_sql(query_plano, conn_origem)

    query_unidade = "SELECT id, nome, empresa_id, id_endereco FROM unidade;"
    df_origem_unidade = pd.read_sql(query_unidade, conn_origem)

    query_endereco = "SELECT id, cep, numero FROM endereco;"
    df_origem_endereco = pd.read_sql(query_endereco, conn_origem)

    query_assinatura = "SELECT id, id_empresa, id_plano, dt_inicio, dt_fim, status FROM assinatura;"
    df_origem_assinatura = pd.read_sql(query_assinatura, conn_origem)

    #-----------------------------------#
    #   DF_DESTINO ( para cada tabela)  #
    #-----------------------------------#
    df_destino_funcionario = pd.DataFrame(columns=['id','nome','sobrenome','email','senha','cargo','setor_id','unidade_id'])

    df_destino_industria = pd.DataFrame(columns=['id','nome','cnpj','email','data_cadastro'])

    df_destino_setor = pd.DataFrame(columns=['id','nome','descricao'])

    df_destino_plano = pd.DataFrame(columns=['id','nome','preco','duracao_meses'])

    df_destino_unidade = pd.DataFrame(columns=['id','nome','cep','rua','bairro','cidade','estado','numero','industria_id'])

    df_destino_assinatura = pd.DataFrame(columns=['id','industria_id','plano_id','data_inicio','data_fim','status'])

    #-------------------------------------#
    # DF_TRASNFORMADO ( para cada tabela) #
    #-------------------------------------#
    df_transformado_funcionario = transformar_dados_funcionario(df_origem_funcionario, df_destino_funcionario)

    df_transformado_industria = transformar_dados_industria(df_origem_industria, df_destino_industria)

    df_transformado_setor = transformar_dados_setor(df_origem_setor,df_destino_setor)

    df_transformado_plano = transformar_dados_planos(df_origem_plano,df_destino_plano)

    df_transformado_unidade = transformar_dados_unidade(df_origem_unidade,df_origem_endereco,df_destino_unidade)

    df_transformado_assinatura = transformar_dados_assinatura(df_origem_assinatura,df_destino_assinatura)

    cursor = conn_destino.cursor()

    # Tratando exeções de Funcionário
    try:

        insert_sql_funcionario = """
            INSERT INTO funcionario (id, nome, sobrenome, email, senha, cargo, setor_id, unidade_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
                row['id'],
                row['nome'],
                row['sobrenome'],
                row['email'],
                row['senha'],
                row['cargo'],
                row['setor_id'],
                row['unidade_id']
            ))

            print("Funcionário OK")

        conn_destino.commit()
    except Exception as e:
        print("Erro ao inserir dados no banco de destino:", e)
        conn_destino.rollback()

    # Tratando exeções de Industria 
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
            cursor.execute(insert_sql_industria, (
                row['id'],
                row['nome'],
                row['cnpj'],
                row['email'],
                row['data_cadastro']
            ))

        conn_destino.commit()

        print("Industria OK")
    except Exception as e:
        print("Erro ao inserir dados no banco de destino:", e)
        conn_destino.rollback()

    # Tratando exeções de Setor
    try:
        insert_sql_setor = """
        INSERT INTO  setor (id,nome,descricao)
        VALUES (%s,%s,%s)
        ON CONFLICT (id) DO UPDATE
            SET nome = EXCLUDED.nome,
                descricao = EXCLUDED.descricao;
        """
        
        for _, row in df_transformado_setor.iterrows():
            cursor.execute(insert_sql_setor, (
                row['id'],
                row['nome'],
                row['descricao']
            ))

        conn_destino.commit()

        print("Setor OK")
    except Exception as e:
        print("Erro ao inserir dados no banco de destino:", e)
        conn_destino.rollback()

# Tratando exceções Plano
    try:
        insert_sql_plano = """
        INSERT INTO plano (id, nome, preco, duracao_meses)
        VALUES (%s, %s, %s,%s)
        ON CONFLICT (id) DO UPDATE
        SET nome = EXCLUDED.nome,
            preco = EXCLUDED.preco,
            duracao_meses = EXCLUDED.duracao_meses;
        """

        for _, row in df_transformado_plano.iterrows():
            cursor.execute(insert_sql_plano, (
                row['id'],
                row['nome'],
                row['preco'],
                row['duracao_meses']
            ))

        conn_destino.commit()
        print("Plano OK")

    except Exception as e:
        print("Erro ao inserir dados no banco de destino:", e)
        conn_destino.rollback()

# Tratando exceções Unidade
    try:
        insert_sql_unidade = """
        INSERT INTO unidade (id, nome, cep, rua, bairro, cidade, estado, numero, industria_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET nome = EXCLUDED.nome,
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
                row['id'],
                row['nome'],
                row['cep'],
                row['rua'],
                row['bairro'],
                row['cidade'],
                row['estado'],
                row['numero'],
                row['industria_id']
            ))

        conn_destino.commit()
        print("Unidades OK")

    except Exception as e:
        print("Erro ao inserir unidades no banco de destino:", e)
        conn_destino.rollback()

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
                row['data_inicio'],
                row['data_fim'],
                row['status']
            ))

        conn_destino.commit()
        print("Assinaturas OK")

    except Exception as e:
        print("Erro ao inserir assinaturas no banco de destino:", e)
        conn_destino.rollback()

    finally:
        cursor.close()
        conn_origem.close()
        conn_destino.close()
    
if __name__ == "__main__":
    main()