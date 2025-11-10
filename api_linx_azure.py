import requests
import pyodbc
import pandas as pd
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# =========================
# 🔧 CONFIGURAÇÕES GERAIS
# =========================
server = "milky-api-server.database.windows.net"
database = "milky_api"
username = "admin_milky"
password = "BMvvPP@4025@"  # ⚠️ pode colocar no Render como variável de ambiente depois
email_destino = "bruno@milkymoo.com.br"

# =========================
# 📅 CONFIGURAÇÃO DE DATAS
# =========================
hoje = datetime.today()
data_final = (hoje - timedelta(days=1)).strftime("%Y-%m-%d")
data_inicial = (hoje - timedelta(days=2)).strftime("%Y-%m-%d")

# =========================
# 💡 TOKEN (exemplo fictício)
# =========================
def gerar_token():
    url = "https://lx-degust-api-integracao-prd.azurewebsites.net/api/token"
    cred = {"usuario": "usuario_api", "senha": "senha_api"}
    resp = requests.post(url, json=cred)
    if resp.status_code == 200:
        return resp.json().get("access_token")
    else:
        raise Exception("Falha ao gerar token")

# =========================
# 🧠 CONSULTA API
# =========================
def buscar_vendas(token, loja_inicial, loja_final, data_inicial, data_final):
    url = "https://lx-degust-api-integracao-prd.azurewebsites.net/api/venda/relatorio-vendas-periodo-sincronizado?api-version=1.0"
    headers = {"accept": "application/json", "Authorization": f"Bearer {token}"}
    payload = {
        "codFranqueador": 2845,
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "tipo": "Intervalo",
        "lojaInicial": loja_inicial,
        "lojaFinal": loja_final,
        "tipoData": "v"
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=120)
    if resp.status_code == 200:
        return resp.json()
    else:
        print(f"❌ Erro ({resp.status_code}) na API para lojas {loja_inicial}-{loja_final}")
        return []

# =========================
# 🧩 INSERÇÃO SQL
# =========================
def inserir_vendas(dados):
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for v in dados:
        cursor.execute("""
            INSERT INTO dbo.vendas_linx_2025 (
                cnpj, codFranqueador, codLoja, datSincronizada, datMovimento, dataVenda,
                controle, tipoVenda, cancelada, valTroco, valDesconto, valDescontoItem, valAcrescimo,
                valProduto, valLiquido, possuiCpfCnpj, statusVenda, situacaoDegust,
                tipoVendaPdv, programaDoacao, justificativaCancelamento, valorDescontoPreCadastrado,
                nomeGerente, vendaHanzo, valorDescontoReshop, outrosDescontos, idSincronyzerDegust,
                temDevolucao, takeout, codigoUsuarioCancelamentoPdvCloud, codigoVendedorPdvCloud, docConsumidor
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            v.get("cnpj"), v.get("codFranqueador"), v.get("codLoja"),
            v.get("datSincronizada"), v.get("datMovimento"), v.get("dataVenda"),
            v.get("controle"), v.get("tipoVenda"), v.get("cancelada"),
            v.get("valTroco"), v.get("valDesconto"), v.get("valDescontoItem"),
            v.get("valAcrescimo"), v.get("valProduto"), v.get("valLiquido"),
            v.get("possuiCpfCnpj"), v.get("statusVenda"), v.get("situacaoDegust"),
            v.get("tipoVendaPdv"), v.get("programaDoacao"), v.get("justificativaCancelamento"),
            v.get("valorDescontoPreCadastrado"), v.get("nomeGerente"), v.get("vendaHanzo"),
            v.get("valorDescontoReshop"), v.get("outrosDescontos"), v.get("idSincronyzerDegust"),
            v.get("temDevolucao"), v.get("takeout"), v.get("codigoUsuarioCancelamentoPdvCloud"),
            v.get("codigoVendedorPdvCloud"), v.get("docConsumidor")
        ))

    conn.commit()
    conn.close()

# =========================
# 📧 ENVIO DE E-MAIL
# =========================
def enviar_email(assunto, corpo):
    msg = MIMEMultipart()
    msg["From"] = "noreply@milkymoo.com.br"
    msg["To"] = email_destino
    msg["Subject"] = assunto
    msg.attach(MIMEText(corpo, "plain"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login("noreply@milkymoo.com.br", "SENHA_DO_EMAIL")
        server.send_message(msg)

# =========================
# 🚀 EXECUÇÃO PRINCIPAL
# =========================
try:
    print(f"🚀 Iniciando atualização LINX: {data_inicial} a {data_final}")
    token = gerar_token()
    print("✅ Token gerado com sucesso")

    total_vendas = 0
    for bloco in range(1, 1201, 50):
        lojas_inicio = bloco
        lojas_fim = bloco + 49
        print(f"🔄 Buscando lojas {lojas_inicio}-{lojas_fim}...")
        vendas = buscar_vendas(token, lojas_inicio, lojas_fim, data_inicial, data_final)
        if vendas:
            inserir_vendas(vendas)
            total_vendas += len(vendas)

    corpo = f"""
    ✅ Atualização LINX concluída!

    Período: {data_inicial} a {data_final}
    Total de vendas importadas: {total_vendas}
    Horário de execução: {datetime.now().strftime('%H:%M:%S')}
    """

    enviar_email("✅ Atualização LINX - Concluída", corpo)
    print("📩 E-mail enviado com sucesso!")

except Exception as e:
    erro = str(e)
    print(f"❌ Erro na atualização: {erro}")
    enviar_email("❌ Erro na atualização LINX", f"Ocorreu um erro:\n\n{erro}")
