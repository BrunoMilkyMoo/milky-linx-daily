import requests
import pandas as pd
import pyodbc
import os
import time
from datetime import datetime, date
from tqdm import tqdm
from requests.exceptions import RequestException

# ========================
# ⚙️ CONFIGURAÇÕES GERAIS
# ========================
DATA_HOJE = date.today().strftime("%Y-%m-%d")

SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DATABASE")
USERNAME = os.getenv("SQL_USER")
PASSWORD = os.getenv("SQL_PASSWORD")
TABELA_DESTINO = "dbo.produto_atual"

# ========================
# 🔑 TOKEN DEGUST
# ========================
def gerar_token():
    url = "https://lx-degust-api-integracao-prd.azurewebsites.net/api/usuario/autenticar?api-version=1.0"
    cred = {"usuario": "02559047110", "senha": "181818", "codigoFranqueador": 2845}
    print("🔑 Gerando novo token...")
    try:
        resp = requests.post(url, json=cred, timeout=30)
        resp.raise_for_status()
        token = resp.json().get("acesso", {}).get("token")
        if not token:
            raise Exception("❌ Token não encontrado na resposta da API.")
        print("✅ Token gerado com sucesso!\n")
        return token
    except Exception as e:
        print(f"❌ Falha ao gerar token: {e}")
        time.sleep(10)
        return gerar_token()

# ========================
# 📦 BUSCAR VENDAS COM RETRIES
# ========================
def buscar_vendas(token, loja, data_inicial, data_final, tentativas=5):
    url = "https://lx-degust-api-integracao-prd.azurewebsites.net/api/venda/relatorio-vendas-periodo-sincronizado?api-version=1.0"
    headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
    payload = {
        "codFranqueador": 2845,
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "tipo": "Lista",
        "listaDeLojas": str(loja),
        "tipoData": "v"
    }

    for tentativa in range(1, tentativas + 1):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=180)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and "vendas" in data:
                return data["vendas"]
            elif isinstance(data, list):
                return data
            else:
                return []
        except RequestException as e:
            print(f"⚠️ Tentativa {tentativa}/{tentativas} falhou (loja {loja}): {e}")
            if tentativa < tentativas:
                print("🔄 Gerando novo token e tentando novamente...")
                token = gerar_token()
                espera = 5 * tentativa
                print(f"⏳ Aguardando {espera}s antes da próxima tentativa...\n")
                time.sleep(espera)
            else:
                print(f"❌ Falha definitiva na loja {loja}\n")
                return []

# ========================
# 💽 ATUALIZAR TABELA SQL
# ========================
def atualizar_produto_atual(df):
    print("💽 Atualizando tabela dbo.produto_atual no SQL...\n")

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};Encrypt=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # 🔁 Limpa e recria tabela se necessário
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

    # Inserção em blocos
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
    print("🏁 Tabela dbo.produto_atual atualizada com sucesso!\n")

# ========================
# 🚀 EXECUÇÃO PRINCIPAL
# ========================
def main():
    inicio_total = time.time()
    hoje = date.today()
    token = gerar_token()

    mes_atual = hoje.month
    ano_atual = hoje.year
    data_inicial = hoje.replace(day=1)
    data_final = hoje

    print(f"\n🚀 Atualizando PRODUTO_ATUAL ({mes_atual}/{ano_atual}) — até {DATA_HOJE}\n")

    registros = []
    for loja in tqdm(range(1, 1101), desc="📦 Coletando lojas"):
        try:
            # 🔁 Regenera token a cada 50 lojas
            if loja % 50 == 0:
                print(f"🔑 Renovando token (loja {loja})...")
                token = gerar_token()

            vendas = buscar_vendas(token, loja, str(data_inicial), str(data_final))
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
                            "Mes": mes,
                            "Ano": ano,
                            "CodigoLoja": cod_loja,
                            "NomeLoja": nome_loja,
                            "CodigoProduto": item.get("codProduto"),
                            "DescricaoProduto": item.get("descricaoProduto"),
                            "Unidade": item.get("unidadeProduto"),
                            "Quantidade": item.get("quantidade") or 0,
                            "ValorTotal": item.get("valTotal") or 0.0
                        })

        except Exception as e:
            print(f"❌ Erro loja {loja}: {e}")

    if not registros:
        print("⚠️ Nenhum registro encontrado.")
        return

    df = pd.DataFrame(registros)
    df_agrupado = (
        df.groupby(
            ["Mes", "Ano", "CodigoLoja", "NomeLoja", "CodigoProduto", "DescricaoProduto", "Unidade"],
            as_index=False
        )
        .agg({"Quantidade": "sum", "ValorTotal": "sum"})
    )

    atualizar_produto_atual(df_agrupado)

    tempo_total = (time.time() - inicio_total) / 60
    print(f"\n🏁 Atualização concluída em {tempo_total:.1f} min — {len(df_agrupado):,} registros.\n")

# ========================
if __name__ == "__main__":
    main()
