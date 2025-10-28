# 🤖 RPA-Fluxar

O **RPA-Fluxar** é uma **automação de processos robóticos (RPA)** desenvolvida pela equipe **NeoTech** como parte do projeto **Fluxar**. Seu objetivo é **coletar dados de um banco de dados*, trata-los e normaliza-los, para depois inserir eles em outro banco de dados

---

## Como Usar

1. **Clone o repositório:**
    ```bash
    git clone [https://github.com/Fluxar-NeoTech/RPA-Fluxar.git](https://github.com/Fluxar-NeoTech/RPA-Fluxar.git)
    cd RPA-Fluxar
    ```
2. **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```
### 3. Configuração do Ambiente

Crie e preencha o arquivo **`.env`** na raiz do projeto com as seguintes variáveis. Estes valores são cruciais para que a automação possa se conectar a serviços externos e ao banco de dados.

---

### Variáveis do Banco de Dados

Estas variáveis são usadas para estabelecer a conexão com os bancos de dados do projeto **Fluxar**.

* **`DB_PASSWORD`**: Senha de acesso ao banco de dados.
    ```env
    DB_PASSWORD="SUA_SENHA_AQUI"
    ```
* **`DB_USER`**: Nome do usuário com permissões de acesso.
    ```env
    DB_USER="SEU_USUARIO_AQUI"
    ```
* **`DB_PORT`**: Porta de comunicação do banco de dados (geralmente `5432` para PostgreSQL).
    ```env
    DB_PORT="5432"
    ```
* **`DB_HOST`**: Endereço ou IP do servidor onde o banco de dados está hospedado.
    ```env
    DB_HOST="localhost" # ou o IP do servidor
    ```
* **`DB_NAME_PRIMEIRO`**: Nome do primeiro banco de dados que a automação irá acessar.
    ```env
    DB_NAME_PRIMEIRO="nome_do_primeiro_banco"
    ```
* **`DB_NAME_SEGUNDO`**: Nome do segundo banco de dados, se aplicável ao seu fluxo de automação.
    ```env
    DB_NAME_SEGUNDO="nome_do_segundo_banco"
    ```

---

### Variáveis de Serviços Externos

Esta variável é necessária para acessar APIs de terceiros.

* **`API_KEY_MAPS`**: Chave de acesso (API Key) para serviços de mapeamento ou geolocalização utilizados na automação.
    ```env
    API_KEY_MAPS="SUA_CHAVE_API_DE_MAPS_AQUI"
    ```

5. **Execute a automação:**
    ```bash
    python app/main.py
    ```

---

## Onde Usar

O RPA-Fluxar pode ser utilizado em **ambientes locais, servidores ou pipelines de automação**, sendo ideal para:

* Extração e transformação de dados;
* Integração entre sistemas e APIs;
* Automação de rotinas administrativas e repetitivas;
* Coleta de informações para análise de dados no projeto Fluxar.

---

## Onde Está Rodando

Atualmente, o sistema pode ser executado:

* **Localmente**, por meio do Python instalado na máquina;
* Em **containers Docker** (com pequenas adaptações);
* Em **servidores com suporte a Python 3.10+**.

---

## Onde Baixar

Você pode acessar ou baixar o projeto diretamente no **GitHub**:

> [https://github.com/Fluxar-NeoTech/RPA-Fluxar](https://github.com/Fluxar-NeoTech/RPA-Fluxar)

--- 

##  Equipe

Projeto desenvolvido pela equipe **NeoTech**
Curso Técnico em Análise de Dados — 2025
**💜 Fluxar: tecnologia que flui com inteligência.**

---
