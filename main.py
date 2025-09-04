import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
import pandas as pd


load_dotenv()

def conectar_banco(serie):
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"), 
            dbname=os.getenv(f"DB_NAME_{serie.upper()}"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        print(f"Connection OK ")
        return conn
    except Exception as e:
        print("Erro ao conectar ao banco:", e)
        exit()

def consultar_dados(serie, query):
    # Abrindo a conexão com o banco de dados 
    conn = conectar_banco(serie)
    # 
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print("Erro ao consultar dados:", e)
        return None
    finally:   
        conn.close()
