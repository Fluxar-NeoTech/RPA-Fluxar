import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
from transforms.funcionario import transformar_dados_funcionario
from utils.db import conectar_banco

load_dotenv()

def main():
    conn_origem = conectar_banco("primeiro")
    conn_destino = conectar_banco("segundo")

    query = "SELECT id, nome, sobrenome, email, senha, cargo, setor_id, unidade_id FROM funcionario;"
    df_origem = pd.read_sql(query, conn_origem)

    df_destino = pd.DataFrame(columns=['id','nome','sobrenome','email','senha','cargo','setor_id','unidade_id'])

    df_transformado = transformar_dados_funcionario(df_origem, df_destino)

    try:
        cursor = conn_destino.cursor()

        insert_sql = """
            INSERT INTO funcionario (id, nome, sobrenome, email, senha, cargo, setor_id, unidade_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df_transformado.iterrows():
            cursor.execute(insert_sql, (
                row['id'],
                row['nome'],
                row['sobrenome'],
                row['email'],
                row['senha'],
                row['cargo'],
                row['setor_id'],
                row['unidade_id']
            ))

        conn_destino.commit()
    except Exception as e:
        print("Erro ao inserir dados no banco de destino:", e)
        conn_destino.rollback()
    finally:
        cursor.close()
        conn_origem.close()
        conn_destino.close()
    
if __name__ == "__main__":
    main()
    