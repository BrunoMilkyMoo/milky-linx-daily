import requests
import pyodbc
import pandas as pd
import smtplib
import os
import time
from datetime import datetime, timedelta, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from tqdm import tqdm

# =======================================
# ⚙️ CONFIGURAÇÕES GERAIS / VARIÁVEIS
# =======================================
SERVER = os.getenv("DB_SERVER", "milky-api-server.database.windows.net")
DATABASE = os.getenv("DB_NAME", "milky_api")
USERNAME = os.getenv("DB_USER", "admin_milky")
PASSWORD = os.getenv("DB_PASS", "BMvvPP@4025@")
EMAIL_REMETENTE = os.getenv("EMAIL_FROM", "diretoria@milkymoo.com.br")
EMAIL_SENHA = os.getenv("EMAIL_PASS", "MilkyMoo20")
EMAIL_DESTINO = os.getenv("EMAIL_TO", "bruno@milkymoo.com.br")

TABELA_DESTINO = "dbo.produto_atual"

# =========================
# 🔑 TOKEN DEGUST
# =========================
def gerar_token():
    url = "https://lx-degust-api-integracao-prd.azurewebsites.net/api/usuario/autenticar?api-version=1.0"
    cred = {"usuario": "02559047110", "senha": "181818", "codigoFranqueador": 2845}
    print("🔑 Gerando token...")
    resp = requests.post(url, json=cred, timeout=30)
    resp.raise_for_status()
    token = resp.json().get("acesso", {}).get("token")
    if not token:
        raise Exception("❌ Token não encontrado na resposta da API.")
    print("✅ Token gerado com sucesso.")
    return token

# =========================
# 🧠 BUSCAR VENDAS
# =========================
def buscar_vendas(token, loja_inicial, loja_final, data_inicial, data_final):
    url = "https://lx-degust-api-integracao-prd.azurewebsites.net/api/venda/relatorio-vendas-periodo-sincronizado?api-version=1.0"
    headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
    payload = {
        "codFranqueador": 2845,
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "tipo": "Intervalo",
        "lojaInicial": loja_inicial,
        "lojaFinal": loja_final,
        "tipoData": "v"
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=180)
    if resp.status_code == 200:
        return resp.json()
    else:
        print(f"❌ Erro ({resp.status_code}) nas lojas {loja_inicial}-{loja_final}")
        return []

# =========================
# 💽 INSERIR / SUBSTITUIR
# =========================
def atualizar_produto_atual(df):
    print("💽 Atualizando tabela dbo.produto_atual no SQL...")
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};Encrypt=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute(f"IF OBJECT_ID('{TABELA_DESTINO}', 'U') IS NOT NULL TRUNCATE TABLE {TABELA_DESTINO};")
    cursor.execute(f"""
        IF OBJECT_ID('{TABELA_DESTINO}', 'U') IS NULL
        CREATE TABLE {TABELA_DESTINO} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            Mes INT,
            Ano INT,
            CodigoLoja INT,
            NomeLoja NVARCHAR(255),
            CodigoProduto NVARCHAR(50),
            DescricaoProduto NVARCHAR(255),
            Unidade NVARCHAR(50),
            Quantidade FLOAT,
            ValorTotal FLOAT
        )
    """)
    conn.commit()

    registros = [tuple(x) for x in df.to_numpy()]
    query = f"""
        INSERT INTO {TABELA_DESTINO} (
            Mes, Ano, CodigoLoja, NomeLoja,
            CodigoProduto, DescricaoProduto,
            Unidade, Quantidade, ValorTotal
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for i in range(0, len(registros), 10000):
        bloco = registros[i:i + 10000]
        cursor.executemany(query, bloco)
        conn.commit()
        print(f"✅ {i + len(bloco):,} registros inseridos...")

    conn.close()
    print("🏁 Atualização concluída no banco.\n")

# =========================
# 📧 ENVIO DE E-MAIL
# =========================
def enviar_email(assunto, corpo):
    msg = MIMEMultipart()
    msg["From"] = EMAIL_REMETENTE
    msg["To"] = EMAIL_DESTINO
    msg["Subject"] = assunto
    msg.attach(MIMEText(corpo, "plain"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(EMAIL_REMETENTE, EMAIL_SENHA)
        server.send_message(msg)

# =========================
# 🚀 EXECUÇÃO PRINCIPAL
# =========================
def main():
    inicio_total = time.time()
    hoje = date.today()
    mes_atual = hoje.month
    ano_atual = hoje.year
    data_inicial = hoje.replace(day=1).strftime("%Y-%m-%d")
    data_final = hoje.strftime("%Y-%m-%d")

    print(f"\n🚀 Atualizando PRODUTO_ATUAL ({mes_atual}/{ano_atual}) — até {data_final}\n")

    token = gerar_token()
    registros = []

    for bloco in tqdm(range(1, 1101, 50), desc="📦 Coletando blocos"):
        lojas_inicio = bloco
        lojas_fim = bloco + 49
        try:
            vendas = buscar_vendas(token, lojas_inicio, lojas_fim, data_inicial, data_final)
            if not vendas:
                continue

            for v in vendas:
                cod_loja = v.get("codLoja")
                nome_loja = v.get("nomeLoja") or ""
                data_venda = v.get("dataVenda", "")
                try:
                    data_obj = datetime.strptime(data_venda, "%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    data_obj = datetime.strptime(data_venda, "%Y-%m-%d")
                mes = data_obj.month
                ano = data_obj.year

                for item in v.get("itens", []):
                    if str(item.get("cancelado", "")).upper() == "N":
                        registros.append({
                            "Mês": mes,
                            "Ano": ano,
                            "Código da Loja": cod_loja,
                            "Nome da Loja": nome_loja,
                            "Código do Produto": item.get("codProduto"),
                            "Descrição do Produto": item.get("descricaoProduto"),
                            "Unidade": item.get("unidadeProduto"),
                            "Quantidade": item.get("quantidade") or 0,
                            "Valor Total": item.get("valTotal") or 0.0
                        })

        except Exception as e:
            print(f"❌ Erro no bloco {lojas_inicio}-{lojas_fim}: {e}")

    if not registros:
        enviar_email("⚠️ Atualização LINX sem dados", "Nenhum registro encontrado no período.")
        print("⚠️ Nenhum registro encontrado.")
        return

    df = pd.DataFrame(registros)
    df_agrupado = (
        df.groupby(
            ["Mês", "Ano", "Código da Loja", "Nome da Loja", "Código do Produto", "Descrição do Produto", "Unidade"],
            as_index=False
        )
        .agg({"Quantidade": "sum", "Valor Total": "sum"})
    )

    atualizar_produto_atual(df_agrupado)

    tempo_total = (time.time() - inicio_total) / 60
    resumo = (
        f"✅ Atualização concluída com sucesso!\n\n"
        f"📅 Período: {data_inicial} a {data_final}\n"
        f"🧾 Total de produtos distintos: {len(df_agrupado):,}\n"
        f"⏱️ Duração: {tempo_total:.1f} minutos.\n"
    )
    print(resumo)
    enviar_email("✅ Atualização LINX Concluída", resumo)

# =========================
if __name__ == "__main__":
    main()
