# -*- coding: utf-8 -*-
import requests
import time
import re
import os
import base64
import tempfile
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
from typing import Dict, Any, Optional, List, Tuple
from zoneinfo import ZoneInfo  # 👈 Fuso horário
from urllib.parse import quote  # ✅ URL-encode do path (corrige 400 no HEAD/POST)

# =========================================================
# === CONFIGURAÇÕES SUPABASE (via REST) ===================
# =========================================================
SUPABASE_URL = "https://hysrxadnigzqadnlkynq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh5c3J4YWRuaWd6cWFkbmxreW5xIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDM3MTQwODAsImV4cCI6MjA1OTI5MDA4MH0.RLcu44IvY4X8PLK5BOa_FL5WQ0vJA3p0t80YsGQjTrA"

TABELA_CERTS = "certifica_dfe"
BUCKET_IMAGENS = "imagens"
PASTA_NOTAS = "notas"  # subpasta dentro do bucket

def supabase_headers(is_json: bool = False) -> Dict[str, str]:
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    if is_json:
        h["Content-Type"] = "application/json"
    return h


# =========================================================
# === CONFIGURAÇÕES GERAIS ================================
# =========================================================

# 🚨 CHAVE ANTI-CAPTCHA
ANTI_CAPTCHA_KEY = "60ce5191cf427863d4f3c79ee20e4afe"

# URLs gerais
URL_HOME  = "https://dfe.sefin.ro.gov.br/"
URL_BASE  = "https://download.dfe.sefin.ro.gov.br"
URL_NOVO  = URL_BASE + "/solicitacoes/novo"
URL_SOLICITACOES       = URL_BASE + "/solicitacoes"
URL_DETALHES_TEMPLATE  = URL_BASE + "/solicitacoes/detalhes/{id}"
URL_CREATE_BASE        = URL_BASE

# Tipos DFe
DFE_TYPES_MAP = {
    "NFe": "0",   # Nota Fiscal Eletrônica
    "CTe": "1",   # Conhecimento de Transporte Eletrônica
    "NFCe": "2",  # Nota Fiscal de Consumidor Eletrônica
}

TIPO_SOLICITACAO = "1"  # 1=PERIODO

MAX_TENTATIVAS = 5
DELAY_ENTRE_TENTATIVAS = 2  # seg

# intervalo entre varreduras (ex.: 36)
INTERVALO_LOOP_SEGUNDOS = 36


# =========================================================
# FUSO HORÁRIO (RONDÔNIA)
# =========================================================
FUSO_RO = ZoneInfo("America/Porto_Velho")

def hoje_ro() -> date:
    """
    Retorna a data de hoje considerando o fuso de Rondônia (America/Porto_Velho),
    independentemente do fuso do servidor (ex.: UTC no Render).
    """
    return datetime.now(FUSO_RO).date()


# =========================================================
# FUNÇÕES AUXILIARES
# =========================================================
def slugify(valor: str) -> str:
    if not valor:
        return "sem-nome"
    s = valor.lower().strip()
    s = s.replace("@", "-at-")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or "sem-nome"

def somente_numeros(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\D+", "", s)

def mes_anterior_codigo() -> str:
    """Retorna código AAAAMM do mês anterior (baseado no fuso de Rondônia)."""
    hoje = hoje_ro()
    inicio_mes_atual = hoje.replace(day=1)
    fim_mes_anterior = inicio_mes_atual - timedelta(days=1)
    return fim_mes_anterior.strftime("%Y%m")

def mes_anterior() -> Tuple[str, str]:
    """Datas do mês anterior no formato DD/MM/YYYY (baseado no fuso de Rondônia)."""
    hoje = hoje_ro()
    inicio_mes_atual = hoje.replace(day=1)
    fim_mes_anterior = inicio_mes_atual - timedelta(days=1)
    inicio_mes_anterior = fim_mes_anterior.replace(day=1)
    return (
        inicio_mes_anterior.strftime("%d/%m/%Y"),
        fim_mes_anterior.strftime("%d/%m/%Y"),
    )

def periodo_mes_anterior_str() -> str:
    """Retorna 'DD/MM/YYYY a DD/MM/YYYY' do mês anterior (fuso de Rondônia)."""
    ini, fim = mes_anterior()
    return f"{ini} a {fim}"

def normalizar_tipo_documento(texto: str) -> Optional[str]:
    """
    Normaliza o texto da coluna 'DOCUMENTO' da listagem para um dos:
      'NFe', 'CTe', 'NFCe'
    """
    if not texto:
        return None
    t = texto.strip().upper()
    t = re.sub(r"\s+", " ", t)

    # CTe
    if "CTE" in t or "CT-E" in t or "CONHECIMENTO" in t:
        return "CTe"
    # NFCe
    if "NFCE" in t or "NFC-E" in t or "CONSUMIDOR" in t:
        return "NFCe"
    # NFe (cuidado com NFCe)
    if "NFE" in t or "NF-E" in t or "NOTA FISCAL" in t:
        if "NFC" in t:
            return "NFCe"
        return "NFe"

    # fallbacks
    if "CTE" in t:
        return "CTe"
    if "NFCE" in t:
        return "NFCe"
    if "NFE" in t:
        return "NFe"

    return None


# =========================================================
# FUNÇÕES SUPABASE: CERTIFICADOS + STORAGE
# =========================================================
def carregar_certificados_validos() -> List[Dict[str, Any]]:
    """Busca certificados em certifica_dfe via REST."""
    url = f"{SUPABASE_URL}/rest/v1/{TABELA_CERTS}"
    params = {
        "select": 'id,pem,key,empresa,codi,user,vencimento,"cnpj/cpf"'
    }
    print("🔎 Buscando certificados na tabela certifica_dfe (REST Supabase)...")
    r = requests.get(url, headers=supabase_headers(), params=params, timeout=30)
    r.raise_for_status()
    certs = r.json() or []
    print(f"   ✔ {len(certs)} certificados encontrados.")
    return certs

def criar_arquivos_cert_temp(cert_row: Dict[str, Any]) -> Tuple[str, str]:
    """Decodifica pem/key em Base64 e grava em arquivos temporários para usar no requests."""
    pem_b64 = cert_row.get("pem") or ""
    key_b64 = cert_row.get("key") or ""

    pem_bytes = base64.b64decode(pem_b64)
    key_bytes = base64.b64decode(key_b64)

    cert_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    key_file = tempfile.NamedTemporaryFile(delete=False, suffix=".key")

    cert_file.write(pem_bytes)
    cert_file.flush()
    cert_file.close()

    key_file.write(key_bytes)
    key_file.flush()
    key_file.close()

    print(f"   ✔ Arquivos temporários de certificado criados: {cert_file.name}, {key_file.name}")
    return cert_file.name, key_file.name


# ---------------------------
# Storage: URLs seguras
# ---------------------------
def _storage_object_url_privado(storage_path: str) -> str:
    """
    Endpoint privado (com Authorization):
      /storage/v1/object/{bucket}/{path}

    ✅ encode preservando "/" mas escapando @, espaços etc.
    """
    storage_path = storage_path.lstrip("/")
    encoded_path = quote(storage_path, safe="/")
    return f"{SUPABASE_URL}/storage/v1/object/{BUCKET_IMAGENS}/{encoded_path}"

def arquivo_ja_existe_no_storage(storage_path: str) -> bool:
    """
    ✅ Checa existência com endpoint privado + URL-encode (corrige 400 por causa de '@')
    Tenta HEAD e, se o servidor não aceitar, tenta GET.
    """
    url = _storage_object_url_privado(storage_path)
    headers = supabase_headers()

    try:
        r = requests.head(url, headers=headers, timeout=20)
        if r.status_code == 200:
            print(f"   ⚠️ Arquivo já existente no storage: {storage_path}")
            return True
        if r.status_code == 404:
            return False

        if r.status_code in (400, 401, 403, 405):
            r2 = requests.get(url, headers=headers, timeout=20, stream=True)
            if r2.status_code == 200:
                print(f"   ⚠️ Arquivo já existente no storage: {storage_path}")
                return True
            if r2.status_code == 404:
                return False

        print(f"   ⚠️ Checagem existência retornou {r.status_code} (HEAD) para {storage_path}")
        return False

    except Exception as e:
        print(f"   ⚠️ Erro ao checar existência no storage ({storage_path}): {e}")
        return False

def upload_para_storage(storage_path: str, conteudo: bytes, content_type: str = "application/zip") -> bool:
    """
    Upload via REST:
      POST /storage/v1/object/{bucket}/{path}

    ✅ path URL-encoded (senão '@' pode quebrar a rota)
    """
    url = _storage_object_url_privado(storage_path)
    headers = supabase_headers()
    headers["Content-Type"] = content_type

    try:
        r = requests.post(url, headers=headers, data=conteudo, timeout=120)
        if r.status_code in (200, 201):
            print(f"   🎉 Upload realizado para Supabase: {storage_path}")
            return True
        else:
            print(f"   ❌ Erro upload ({r.status_code}) {storage_path}: {r.text}")
            return False
    except Exception as e:
        print(f"   ❌ Erro ao fazer upload para Supabase ({storage_path}): {e}")
        return False

def montar_nome_final_arquivo(
    base_name: str,
    empresa: str,
    user: str,
    codi: Optional[int],
    mes_cod: str,
    doc: str,
) -> str:
    """
    Padrão:
      AAAAMM-<codi>-<doc>-<email>-<base_name>
    """
    doc_clean = somente_numeros(doc) or "sem-doc"
    cod_str = str(codi) if codi is not None else "0"
    email = user or "sem-user"  # mantém e-mail cru
    return f"{mes_cod}-{cod_str}-{doc_clean}-{email}-{base_name}"


# =========================================================
# SESSÃO (mTLS)
# =========================================================
def criar_sessao(cert_path: str, key_path: str) -> requests.Session:
    s = requests.Session()
    try:
        s.cert = (cert_path, key_path)
        print("✅ Certificado e chave carregados com sucesso.")
    except Exception as e:
        print(f"❌ ERRO FATAL AO CARREGAR CERTIFICADOS: {e}")
        raise

    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    return s


# =========================================================
# ANTI-CAPTCHA
# =========================================================
def resolver_captcha_anticaptcha(b64_image_content: str) -> Optional[str]:
    if not ANTI_CAPTCHA_KEY:
        print("⚠️ ANTI_CAPTCHA_KEY está vazia. Usando modo manual.")
        return None

    print("🤖 Tentando Anti-Captcha API...")
    start_time = time.time()

    payload: Dict[str, Any] = {
        "clientKey": ANTI_CAPTCHA_KEY,
        "task": {
            "type": "ImageToTextTask",
            "body": b64_image_content,
            "phrase": False,
            "case": True,
            "numeric": 0,
        },
    }

    try:
        r = requests.post("https://api.anti-captcha.com/createTask", json=payload, timeout=20)
        r.raise_for_status()
        resp = r.json()
        task_id = resp.get("taskId")

        if not task_id:
            print("❌ Erro ao criar task no Anti-Captcha (Chave inválida?):", resp)
            return None

        for _ in range(5):
            time.sleep(3)
            r = requests.post(
                "https://api.anti-captcha.com/getTaskResult",
                json={"clientKey": ANTI_CAPTCHA_KEY, "taskId": task_id},
                timeout=20,
            )
            r.raise_for_status()
            result = r.json()
            status = result.get("status")

            if status == "ready":
                text = result["solution"]["text"]
                print(f"✅ Anti-Captcha resolveu em {time.time() - start_time:.1f}s: {text}")
                return text

            if status != "processing":
                print("❌ Anti-CAPTCHA retornou erro:", result)
                return None

        print("❌ Anti-Captcha não conseguiu resolver o captcha a tempo (Timeout).")
        return None

    except Exception as e:
        print("❌ Erro de conexão/API com Anti-Captcha:", e)
        return None


# =========================================================
# PARTE 1 — CRIAR SOLICITAÇÕES (NFe, CTe, NFCe) DO MÊS ANTERIOR
# =========================================================
def extrair_tokens_e_captcha(html: str) -> Tuple[str, str, str, str, bytes, str]:
    soup = BeautifulSoup(html, "html.parser")

    csrf = soup.find("meta", {"name": "csrf-token"})
    csrf_token = csrf["content"] if csrf else None

    token_input = soup.find("input", {"name": "token"})
    token_captcha = token_input["value"] if token_input else None

    id_pessoa = soup.find("input", {"name": "id_pessoa"})
    cnpj_completo = id_pessoa["value"] if id_pessoa else None
    cnpj_limpo = re.sub(r"\D", "", cnpj_completo) if cnpj_completo else None

    form = soup.find("form", {"id": "frm_solicitacao"})
    action_url_relative = form.get("action") if form and form.get("action") else None
    if not action_url_relative:
        raise Exception("ERRO: O atributo 'action' do formulário está vazio.")

    URL_CREATE = URL_CREATE_BASE + action_url_relative

    img = soup.find("img", src=re.compile(r"^data:image/png;base64"))
    if not img:
        raise Exception("ERRO: Imagem do CAPTCHA não encontrada.")

    b64 = img["src"].split(",")[1]
    img_bytes = base64.b64decode(b64)

    print(f"   ✅ Tokens extraídos. CNPJ: {cnpj_limpo}")

    if not csrf_token or not token_captcha or not cnpj_limpo:
        raise Exception("Erro na extração dos tokens de segurança (CSRF, Token, CNPJ).")

    return csrf_token, token_captcha, cnpj_limpo, URL_CREATE, img_bytes, b64


def enviar_solicitacao_unica(s: requests.Session, dfe_name: str, dfe_type_code: str) -> bool:
    print("\n========================================================")
    print(f"🚀 INICIANDO SOLICITAÇÃO: {dfe_name} (Tipo: {dfe_type_code})")
    print("========================================================")

    print("👉 1. Acessando NOVA SOLICITAÇÃO para obter tokens e CAPTCHA...")
    start_total_time = time.time()
    r_novo = s.get(URL_NOVO, timeout=30, allow_redirects=True)

    if r_novo.status_code != 200:
        print(f"   ERRO: Status {r_novo.status_code}. Não foi possível carregar a página.")
        return False

    try:
        csrf_token, token_captcha, cnpj_limpo, URL_CREATE, img_bytes, b64_captcha = extrair_tokens_e_captcha(r_novo.text)
    except Exception as e:
        print(f"❌ Erro na extração dos dados: {e}")
        return False

    captcha_resposta: Optional[str] = resolver_captcha_anticaptcha(b64_captcha)

    if not captcha_resposta:
        print("\n====================================================================")
        print("🛑 MODO MANUAL: Resolução automática falhou ou não configurada.")
        print("====================================================================")
        captcha_resposta = input(f"Digite o CAPTCHA para {dfe_name}: ").strip()
        if not captcha_resposta:
            print("❌ Nenhuma resposta de CAPTCHA fornecida. Abortando POST.")
            return False

    data_ini, data_fim = mes_anterior()

    payload: Dict[str, str] = {
        "authenticity_token": csrf_token,
        "token": token_captcha,
        "captcha_resposta": captcha_resposta,
        "id_pessoa": cnpj_limpo,
        "tp_solicitacao": TIPO_SOLICITACAO,
        "dfe_documento": dfe_type_code,
        "dfe_status[ativo]": "1",
        "dfe_status[cancelado]": "1",
        "periodo_inicial": data_ini,
        "periodo_final": data_fim,
        "dfes": "",
    }

    print(f"\n   Payload pronto. Tipo: {dfe_name} | Período: {data_ini} a {data_fim}")

    headers: Dict[str, str] = {
        "Referer": URL_NOVO,
        "X-CSRF-Token": csrf_token,
        "X-Requested-With": "XMLHttpRequest",
    }

    print("\n👉 2. Enviando solicitação POST...")
    total_elapsed_time = time.time() - start_total_time
    print(f"⏱️ TEMPO TOTAL GASTO ANTES DO POST: {total_elapsed_time:.2f} segundos.")

    r_post = s.post(URL_CREATE, data=payload, headers=headers, timeout=60, allow_redirects=False)
    print(f"   Status FINAL do POST: {r_post.status_code}")

    success = False
    if r_post.status_code == 302:
        redirect_url = r_post.headers.get("Location", "URL de Redirecionamento não encontrada")
        print("🎉 SUCESSO COMPLETO (302 REDIRECIONAMENTO).")
        print(f"   Solicitação de {dfe_name} aceita e enviada para: {redirect_url}")
        success = True
    elif r_post.status_code == 200:
        response_text = r_post.text.strip()
        print(f"   Resposta do Servidor (200): {response_text}")

        if response_text == '{"status":"Texto de verificação inválido"}':
            print(f"❌ ERRO CRÍTICO: 'Texto de verificação inválido' ({dfe_name}).")
        elif '"status":"ok"' in response_text or '"status":"success"' in response_text:
            print(f"✅ SUCESSO: Solicitação de {dfe_name} aceita (Status JSON OK/SUCCESS).")
            success = True
        else:
            print(f"🛑 ERRO DE VALIDAÇÃO (200): verificar conteúdo para {dfe_name}.")
    else:
        print(f"🛑 ERRO INESPERADO: Status {r_post.status_code} em {dfe_name}.")

    return success


def enviar_solicitacao_sequencial(s: requests.Session, apenas_tipos: Optional[List[str]] = None):
    """
    Abre solicitações do mês anterior. Se 'apenas_tipos' informado, solicita só os faltantes.
      ex: ['NFe','CTe']
    """
    if apenas_tipos is None:
        tipos = list(DFE_TYPES_MAP.items())
    else:
        tipos = [(k, DFE_TYPES_MAP[k]) for k in apenas_tipos if k in DFE_TYPES_MAP]

    print("\n=== INICIANDO ABERTURA DE NOVAS SOLICITAÇÕES (MÊS ANTERIOR) ===")
    for dfe_name, dfe_type_code in tipos:
        tentativas = 0
        success = False

        while tentativas < MAX_TENTATIVAS and not success:
            if tentativas > 0:
                print(f"\n--- TENTATIVA {tentativas + 1} de {MAX_TENTATIVAS} para {dfe_name} ---")
                time.sleep(DELAY_ENTRE_TENTATIVAS)

            success = enviar_solicitacao_unica(s, dfe_name, dfe_type_code)
            tentativas += 1

        if not success:
            print(f"\n[SEQUÊNCIA] {dfe_name} falhou após {MAX_TENTATIVAS} tentativas.")
        else:
            print(f"\n[SUCESSO] {dfe_name} solicitado.")
            print(f"[PAUSA] Aguardando {DELAY_ENTRE_TENTATIVAS} segundos antes do próximo tipo.")
            time.sleep(DELAY_ENTRE_TENTATIVAS)


# =========================================================
# PARTE 2 — LISTAR TODAS AS SOLICITAÇÕES
# =========================================================
def listar_solicitacoes(s: requests.Session) -> List[Dict[str, str]]:
    """
    Lista TODAS as solicitações (independente de estado),
    para depois filtrar por período + tipo + doc no fluxo.
    """
    print("🔎 Acessando lista de solicitações...")
    r = s.get(URL_SOLICITACOES, timeout=30)
    if r.status_code != 200:
        print("❌ Erro ao acessar /solicitacoes:", r.status_code)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    tabela = soup.find("table", {"class": "table-hover"})
    if not tabela:
        print("⚠️ Tabela de solicitações não encontrada.")
        return []

    headers = [th.text.strip().upper() for th in tabela.find_all("th")]
    header_map = {name: i for i, name in enumerate(headers)}
    idx_data = header_map.get("DATA")
    idx_doc = header_map.get("DOCUMENTO")
    idx_status = header_map.get("ESTADO")
    idx_acoes = header_map.get("AÇÕES")

    if None in [idx_data, idx_doc, idx_status, idx_acoes]:
        print("❌ Cabeçalhos esperados não encontrados (DATA, DOCUMENTO, ESTADO, AÇÕES).")
        return []

    itens: List[Dict[str, str]] = []
    rows = tabela.find("tbody").find_all("tr") if tabela.find("tbody") else tabela.find_all("tr")[1:]

    for row in rows:
        cols = row.find_all("td")
        if len(cols) <= max(idx_data, idx_doc, idx_status, idx_acoes):
            continue

        data_full = cols[idx_data].text.strip()
        tipo_documento = cols[idx_doc].text.strip()
        estado = cols[idx_status].text.strip().upper()

        detalhe_link = cols[idx_acoes].find("a", href=re.compile(r"/solicitacoes/detalhes/(\d+)"))
        solicitacao_id = None
        if detalhe_link and detalhe_link.has_attr("href"):
            m = re.search(r"/solicitacoes/detalhes/(\d+)", detalhe_link["href"])
            if m:
                solicitacao_id = m.group(1)

        if solicitacao_id:
            itens.append({
                "id": solicitacao_id,
                "documento": tipo_documento,  # texto da tabela
                "estado": estado,
                "data": data_full,
                "file_name": f"{tipo_documento}_{solicitacao_id}.zip".replace(" ", "_").replace("/", "-"),
            })

    print(f"   ✔ {len(itens)} solicitações encontradas na tabela.")
    return itens


# =========================================================
# DETALHES: PERÍODO + DOC (quando existir)
# =========================================================
def extrair_detalhes_solicitacao(s: requests.Session, solicitacao_id: str) -> Dict[str, Optional[str]]:
    """
    Abre detalhes e tenta extrair:
      - periodo (PERÍODO)
      - doc (CNPJ/CPF) se aparecer
    """
    url = URL_DETALHES_TEMPLATE.format(id=solicitacao_id)
    r = s.get(url, timeout=30)
    if r.status_code != 200:
        print(f"   ❌ Erro ao abrir Detalhes {solicitacao_id}: {r.status_code}")
        return {"periodo": None, "doc": None}

    soup = BeautifulSoup(r.text, "lxml")
    tabela = soup.find("table", class_=re.compile("table-xxs"))
    if not tabela:
        # não falha geral, só retorna N/D
        return {"periodo": None, "doc": None}

    periodo = None
    doc = None

    for tr in tabela.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        k = tds[0].get_text(strip=True).upper()
        v = tds[1].get_text(strip=True)

        if "PERÍODO" in k:
            periodo = v

        if ("CNPJ" in k) or ("CPF" in k):
            doc = somente_numeros(v) or doc

    return {"periodo": periodo, "doc": doc}


# =========================================================
# DOWNLOAD
# =========================================================
def obter_url_captcha(s: requests.Session, solicitacao_id: str) -> Optional[Tuple[str, str]]:
    detalhes_url = URL_DETALHES_TEMPLATE.format(id=solicitacao_id)
    r = s.get(detalhes_url, timeout=30)
    if r.status_code != 200:
        print("❌ Erro ao carregar Detalhes:", r.status_code)
        return None

    soup = BeautifulSoup(r.text, "lxml")
    link = soup.find(
        "a",
        class_=re.compile(r"\blink-detalhe\b"),
        href=re.compile(r"get_captcha_download"),
    )

    if not link or not link.has_attr("href"):
        print("❌ Não achei o link do ARQUIVO (get_captcha_download) na página de Detalhes.")
        return None

    href = link["href"]
    captcha_url = URL_BASE + href if not href.startswith("http") else href
    print(f"   ✅ URL do pop-up (get_captcha_download): {captcha_url}")
    return captcha_url, detalhes_url


def extrair_html_modal(js: str) -> Optional[str]:
    m = re.search(r'\$\("#bloco_modal"\)\.html\((?P<q>[\'"])(.+?)(?P=q)\)', js, re.S)
    if m:
        raw = m.group(2)
        html_modal = (
            raw.replace("\\n", "\n")
               .replace("\\t", "\t")
               .replace('\\"', '"')
               .replace("\\/", "/")
        )
        return html_modal

    if "<form" in js and "captcha_resposta" in js:
        return js

    print("❌ HTML do modal não encontrado.")
    return None


def realizar_download_dfe(s: requests.Session, solicitacao_data: Dict[str, str], storage_path: str) -> bool:
    solicitacao_id = solicitacao_data["id"]
    print("\n⬇️ Iniciando download do ID", solicitacao_id)

    res = obter_url_captcha(s, solicitacao_id)
    if not res:
        return False
    captcha_url, detalhes_url = res

    r_get = s.get(
        captcha_url,
        headers={
            "Referer": detalhes_url,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "text/html,*/*",
        },
        timeout=30,
    )
    if r_get.status_code != 200:
        print("❌ Erro HTTP ao buscar get_captcha_download:", r_get.status_code)
        return False

    js = r_get.text
    html_modal = extrair_html_modal(js)
    if not html_modal:
        return False

    soup = BeautifulSoup(html_modal, "lxml")
    form = soup.find("form")
    token_input = form.find("input", {"name": "token"}) if form else None
    img_tag = form.find("img") if form else None

    if not form or not token_input or not img_tag or not img_tag.get("src", "").startswith("data:image"):
        print("❌ Elementos críticos (Formulário, Token ou Imagem CAPTCHA) não encontrados no modal.")
        return False

    action = form.get("action")
    if not action.startswith("http"):
        action = URL_BASE + action

    token = token_input["value"]
    b64 = img_tag["src"].split(",", 1)[1]

    captcha = resolver_captcha_anticaptcha(b64)
    if not captcha:
        print("🛑 Resolução automática falhou. Modo manual.")
        captcha = input("Digite o CAPTCHA: ").strip()
        if not captcha:
            print("❌ Nenhuma resposta de CAPTCHA fornecida.")
            return False

    print("3️⃣ Enviando GET final para baixar o ZIP...")
    params = {"token": token, "captcha_resposta": captcha}
    r_final = s.get(action, params=params, stream=True, timeout=120)

    content_type = (r_final.headers.get("Content-Type") or "").lower()
    if "application/zip" in content_type or "application/octet-stream" in content_type:
        conteudo = b"".join(chunk for chunk in r_final.iter_content(8192) if chunk)
        return upload_para_storage(storage_path, conteudo, content_type="application/zip")

    print("❌ Falha no GET final. Content-Type:", content_type, "| Status:", r_final.status_code)
    return False


# =========================================================
# FLUXO POR EMPRESA (PERÍODO + TIPO + DOC) + NÃO DUPLICAR
# =========================================================
def fluxo_completo_para_empresa(cert_row: Dict[str, Any]):
    empresa = cert_row.get("empresa") or ""
    user = cert_row.get("user") or ""
    codi = cert_row.get("codi")
    venc = cert_row.get("vencimento")
    doc = cert_row.get("cnpj/cpf") or ""
    doc_alvo = somente_numeros(doc) or ""

    print("\n\n========================================================")
    print(f"🏢 Iniciando fluxo para empresa: {empresa} | user: {user} | codi: {codi} | doc: {doc} | venc: {venc}")
    print("========================================================")

    try:
        cert_path, key_path = criar_arquivos_cert_temp(cert_row)
        s = criar_sessao(cert_path, key_path)
    except Exception as e:
        print("❌ Erro ao criar sessão com certificado:", e)
        return

    print("--- INICIANDO VERIFICAÇÃO / SOLICITAÇÕES / DOWNLOADS ---")
    solicitacoes = listar_solicitacoes(s)

    periodo_alvo = periodo_mes_anterior_str()
    mes_cod = mes_anterior_codigo()

    # Controla “já existe solicitação do mês anterior” POR TIPO para este doc
    existe_por_tipo: Dict[str, bool] = {k: False for k in DFE_TYPES_MAP.keys()}

    if solicitacoes:
        for item in solicitacoes:
            solicitacao_id = item["id"]
            estado = (item.get("estado") or "").upper()

            tipo_norm = normalizar_tipo_documento(item.get("documento", ""))
            if not tipo_norm or tipo_norm not in DFE_TYPES_MAP:
                continue

            det = extrair_detalhes_solicitacao(s, solicitacao_id)
            periodo = (det.get("periodo") or "").strip()
            doc_det = somente_numeros(det.get("doc")) if det.get("doc") else ""

            # Filtro período
            if not periodo or periodo != periodo_alvo:
                continue

            # Filtro doc (só se o detalhe trouxer doc)
            if doc_alvo and doc_det and doc_det != doc_alvo:
                continue

            existe_por_tipo[tipo_norm] = True
            print(f"✔ Solicitação {solicitacao_id} | tipo: {tipo_norm} | período: {periodo} | estado: {estado} | doc(det): {doc_det or 'N/D'}")

            if estado == "DOWNLOAD":
                base_name = item["file_name"]
                nome_final = montar_nome_final_arquivo(
                    base_name=base_name,
                    empresa=empresa,
                    user=user,
                    codi=codi,
                    mes_cod=mes_cod,
                    doc=doc_alvo or doc,
                )
                storage_path = f"{PASTA_NOTAS}/{nome_final}"

                # ✅ aqui é o ponto que estava causando duplicação: agora detecta certo mesmo com "@"
                if arquivo_ja_existe_no_storage(storage_path):
                    print(f"   ⤵ Já existe no Supabase, não será baixado novamente: {storage_path}")
                    continue

                ok = False
                tent = 0
                while tent < 3 and not ok:
                    ok = realizar_download_dfe(s, item, storage_path)
                    tent += 1
                    if not ok:
                        print(f"   Tentativa {tent} falhou para ID {solicitacao_id}.")
                        time.sleep(10)

                if ok:
                    print(f"   ✅ Download concluído para ID {solicitacao_id}.")
                else:
                    print(f"❌ Falha crítica ao baixar ID {solicitacao_id} depois de 3 tentativas.")
            else:
                print(f"   🔄 ID {solicitacao_id} ainda está em '{estado}'. Aguardando próxima varredura.")

        # Abrir novas solicitações só para tipos faltantes
        faltando = [k for k, v in existe_por_tipo.items() if not v]
        if not faltando:
            print("\n✅ Já existe solicitação do MÊS ANTERIOR para ESTE DOC em TODOS os tipos (NFe/CTe/NFCe).")
            print("   ❌ Não será aberta nova solicitação agora (evita duplicar pedidos).")
        else:
            print("\n⚠️ Estão faltando solicitações do mês anterior para este DOC nos tipos:", ", ".join(faltando))
            print("➡️ Abrindo novas solicitações SOMENTE para os tipos faltantes...")
            enviar_solicitacao_sequencial(s, apenas_tipos=faltando)

    else:
        print("\n⚠️ Nenhuma solicitação encontrada na lista.")
        print("➡️ Abrindo novas solicitações para o mês anterior...")
        enviar_solicitacao_sequencial(s)


# =========================================================
# MAIN: PROCESSA TODAS AS EMPRESAS DA certifica_dfe EM LOOP
# =========================================================
def processar_todas_empresas():
    certs = carregar_certificados_validos()
    if not certs:
        print("⚠️ Nenhum certificado encontrado na tabela certifica_dfe.")
        return

    for cert_row in certs:
        try:
            fluxo_completo_para_empresa(cert_row)
        except Exception as e:
            print(f"❌ Erro inesperado ao processar empresa {cert_row.get('empresa')}: {e}")


if __name__ == "__main__":
    while True:
        print("\n\n==================== NOVA VARREDURA GERAL ====================")
        print(f"📅 Data (fuso RO): {hoje_ro().strftime('%d/%m/%Y')}")
        try:
            processar_todas_empresas()
        except Exception as e:
            print(f"💥 Erro inesperado no loop principal: {e}")
        print(f"🕒 Aguardando {INTERVALO_LOOP_SEGUNDOS} segundos para próxima varredura...\n")
        time.sleep(INTERVALO_LOOP_SEGUNDOS)
