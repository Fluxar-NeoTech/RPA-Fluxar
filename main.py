import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os


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

        
conectar_banco("PRIMEIRO")
# Criar um cursor
# cur = conectar_banco.cursor()