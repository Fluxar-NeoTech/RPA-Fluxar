import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

# ======================
# Conexão e utilitários
# ======================
def conectar_banco(serie):
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"), 
            dbname=os.getenv(f"DB_NAME_{serie.upper()}"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        print(f"Connection OK - {serie}")
        return conn
    except Exception as e:
        print("Erro ao conectar ao banco:", e)
        exit()

def consultar_dados(serie, query):
    conn = conectar_banco(serie)
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"⚠️ Erro ao consultar dados ({serie}):", e)
        return None
    finally:   
        conn.close()

def limpar_tabela(serie, tabela):
    conn = conectar_banco(serie)
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {tabela} RESTART IDENTITY CASCADE;")
        conn.commit()
        print(f"✅ Tabela {tabela} limpa no banco {serie}")
    except Exception as e:
        print(f"⚠️ Erro ao limpar tabela {tabela}:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def inserir_dados(serie, tabela, df: pd.DataFrame):
    if df.empty:
        print(f"⚠️ Nenhum dado para inserir em {tabela}")
        return
    
    conn = conectar_banco(serie)
    cursor = conn.cursor()
    try:
        colunas = list(df.columns)
        values_placeholders = ", ".join(["%s"] * len(colunas))
        colunas_str = ", ".join(colunas)

        insert_sql = f"INSERT INTO {tabela} ({colunas_str}) VALUES ({values_placeholders})"

        for _, row in df.iterrows():
            cursor.execute(insert_sql, tuple(row))

        conn.commit()
        print(f"✅ {len(df)} registros inseridos em {tabela} no banco {serie}")
    except Exception as e:
        print(f"⚠️ Erro ao inserir dados em {tabela}:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# ======================
# Transformações
# ======================
def transformar_dados_planos(df: pd.DataFrame) -> pd.DataFrame:
    df['nome'] = df['nome'].str.strip().str.title() if 'nome' in df.columns else 'Plano'
    df['preco'] = df['preco'].apply(lambda x: max(x, 0)) if 'preco' in df.columns else 0
    df['duracao_meses'] = df['duracao'] if 'duracao' in df.columns else 1
    return df[['id', 'nome', 'preco', 'duracao_meses']]

def transformar_dados_industria(df: pd.DataFrame) -> pd.DataFrame:
    df['nome'] = df['nome'].str.strip().str.title().str[:100] if 'nome' in df.columns else 'Indústria'
    df['email'] = df['email'].str.strip().str.lower().str[:100] if 'email' in df.columns else 'contato@exemplo.com'
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro'], errors='coerce') if 'data_cadastro' in df.columns else pd.Timestamp.now()
    df['cnpj'] = df['cnpj'] if 'cnpj' in df.columns else '00.000.000/0000-00'
    return df[['id', 'nome', 'cnpj', 'email', 'data_cadastro']].drop_duplicates()

def transformar_dados_funcionario(df: pd.DataFrame) -> pd.DataFrame:
    df['id'] = df['id'].astype(int) if 'id' in df.columns else range(1, len(df)+1)
    df['nome'] = df['nome'].str.strip().str.title().str[:100] if 'nome' in df.columns else 'Nome'
    df['sobrenome'] = df['sobrenome'].str.strip().str.title().str[:100] if 'sobrenome' in df.columns else 'Sobrenome'
    df['email'] = df['email'].str.strip().str.lower().str[:100] if 'email' in df.columns else 'email@exemplo.com'
    
    def mapear_cargo(valor):
        v = str(valor).upper()
        if "G" in v:
            return "G"
        elif "A" in v:
            return "A"
        return "F"  # default funcionário

    df['cargo'] = df['cargo'].apply(mapear_cargo) if 'cargo' in df.columns else "F"
    df['senha'] = df['senha'].astype(str) if 'senha' in df.columns else "123456"
    df['setor_id'] = df['setor_id'] if 'setor_id' in df.columns else 0  
    df['unidade_id'] = df['unidade_id'] if 'unidade_id' in df.columns else 0  


    return df[['id', 'nome', 'sobrenome', 'email', 'senha', 'cargo', 'setor_id', 'unidade_id']]

def transformar_dados_unidade(df: pd.DataFrame) -> pd.DataFrame:
    df_destino = pd.DataFrame()
    df_destino['id'] = df['id'] if 'id' in df.columns else range(1, len(df)+1)
    df_destino['nome'] = df['nome'].str.strip().str.title().str[:100] if 'nome' in df.columns else 'Unidade'
    df_destino['cep'] = df['cep'] if 'cep' in df.columns else '00000000'
    df_destino['numero'] = df['numero'] if 'numero' in df.columns else 0

    df['endereco'] = df['endereco'].fillna('').str.strip() if 'endereco' in df.columns else ''
    partes = df['endereco'].str.split(' ', expand=True)
    df_destino['rua'] = partes[0] if partes.shape[1] > 0 else 'Rua X'
    df_destino['cidade'] = partes[1] if partes.shape[1] > 1 else 'Cidade'
    df_destino['estado'] = partes[2] if partes.shape[1] > 2 else 'Estado'
    df_destino['bairro'] = partes[3] if partes.shape[1] > 3 else 'Bairro'
    return df_destino

# ======================
# Fluxo principal
# ======================
def main():
    banco_origem = "primeiro"
    banco_destino = "segundo"

    tabelas = {
        "funcionario": ("SELECT * FROM funcionario;", transformar_dados_funcionario),
        "industria": ("SELECT * FROM industria;", transformar_dados_industria),
        "unidade": ("SELECT * FROM unidade;", transformar_dados_unidade),
        "planos": ("SELECT * FROM planos;", transformar_dados_planos)
    }

    for nome, (query, func_transformar) in tabelas.items():
        print(f"\n=== Processando tabela: {nome} ===")

        df_temp = consultar_dados(banco_origem, query)

        if df_temp is None or df_temp.empty:
            print(f"⚠️ Tabela {nome} não existe ou está vazia no banco origem.")
            continue

        df_final = func_transformar(df_temp)

        limpar_tabela(banco_destino, nome)
        inserir_dados(banco_destino, nome, df_final)

    print("\n✅ Sincronização concluída!")

if __name__ == "__main__":
    main()
