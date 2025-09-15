import pandas as pd
from dotenv import load_dotenv
from transforms.funcionario import transformar_dados_funcionario
from transforms.industria import transformar_dados_industria
from transforms.setor import transformar_dados_setor
from utils.db import conectar_banco

load_dotenv()

def main():
    conn_origem = conectar_banco("primeiro")
    conn_destino = conectar_banco("segundo")

    query_funcionario = "SELECT id, nome, sobrenome, email, senha, cargo, setor_id, unidade_id FROM funcionario;"
    df_origem_funcionario = pd.read_sql(query_funcionario, conn_origem)

    query_industria = "SELECT id, nome, cnpj, email, data_cadastro FROM industria;"
    df_origem_industria = pd.read_sql(query_industria, conn_origem)

    query_setor = "SELECT id,nome FROM setor;"
    df_origem_setor = pd.read_sql(query_setor, conn_origem)

    df_destino_funcionario = pd.DataFrame(columns=['id','nome','sobrenome','email','senha','cargo','setor_id','unidade_id'])
    df_destino_industria = pd.DataFrame(columns=['id','nome','cnpj','email','data_cadastro'])
    df_destino_setor = pd.DataFrame(columns=['id','nome'])

    df_transformado_funcionario = transformar_dados_funcionario(df_origem_funcionario, df_destino_funcionario)
    df_transformado_industria = transformar_dados_industria(df_origem_industria, df_destino_industria)
    df_transformado_setor = transformar_dados_setor(df_origem_setor,df_destino_setor)
    
    
    cursor = conn_destino.cursor()

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

    try:
        insert_sql_setor = """
        INSERT INTO  setor (id,nome)
        VALUES (%s,%s)
        ON CONFLICT (id) DO UPDATE
            SET nome = EXCLUDED.nome;
    """
        
        for _, row in df_transformado_setor.iterrows():
            cursor.execute(insert_sql_industria, (
                row['id'],
                row['nome']
            ))

        conn_destino.commit()

        print("Setor OK")

    finally:
        cursor.close()
        conn_origem.close()
        conn_destino.close()
    
if __name__ == "__main__":
    main()