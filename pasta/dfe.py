# -*- coding: utf-8 -*-
"""
ROBÔ ÚNICO:
- DF-e RO (SEFIN/RO download.dfe.sefin.ro.gov.br): solicita mês anterior + baixa ZIP quando pronto
- NFS-e ADN (adn.nfse.gov.br): varre NSU, extrai XMLs do mês anterior, zipa e sobe pro Supabase

Flags (Render -> Environment):
  RUN_DFE=1   (default 1)
  RUN_NFSE=1  (default 1)

  ANTI_CAPTCHA_KEY=...
  HTTP_PROXY / HTTPS_PROXY (opcional)

  START_NSU=0
  MAX_NSU=400
  INTERVALO_LOOP_SEGUNDOS=900
"""

import os
import re
import time
import json
import base64
import gzip
import socket
import zipfile
import tempfile
import shutil
import requests

from datetime import date, timedelta, datetime
from typing import Dict, Any, Optional, List, Tuple
from zoneinfo import ZoneInfo

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bs4 import BeautifulSoup
from lxml import etree


# =========================================================
# === SUPABASE (via REST) =================================
# =========================================================
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://hysrxadnigzqadnlkynq.supabase.co").strip()
SUPABASE_KEY = os.getenv(
    "SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh5c3J4YWRuaWd6cWFkbmxreW5xIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDM3MTQwODAsImV4cCI6MjA1OTI5MDA4MH0.RLcu44IvY4X8PLK5BOa_FL5WQ0vJA3p0t80YsGQjTrA"
).strip()

TABELA_CERTS = os.getenv("TABELA_CERTS", "certifica_dfe").strip()
BUCKET_IMAGENS = os.getenv("BUCKET_IMAGENS", "imagens").strip()
PASTA_NOTAS = os.getenv("PASTA_NOTAS", "notas").strip()

def supabase_headers(is_json: bool = False) -> Dict[str, str]:
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    if is_json:
        h["Content-Type"] = "application/json"
    return h


# =========================================================
# === FLAGS / LOOP ========================================
# =========================================================
RUN_DFE  = (os.getenv("RUN_DFE", "1").strip() == "1")
RUN_NFSE = (os.getenv("RUN_NFSE", "1").strip() == "1")

START_NSU_DEFAULT = int(os.getenv("START_NSU", "0") or "0")
MAX_NSU_DEFAULT   = int(os.getenv("MAX_NSU", "400") or "400")
INTERVALO_LOOP_SEGUNDOS = int(os.getenv("INTERVALO_LOOP_SEGUNDOS", "900") or "900")  # 15min default


# =========================================================
# FUSO HORÁRIO (RONDÔNIA)
# =========================================================
FUSO_RO = ZoneInfo("America/Porto_Velho")

def hoje_ro() -> date:
    return datetime.now(FUSO_RO).date()

def mes_anterior_codigo() -> str:
    hoje = hoje_ro()
    inicio_mes_atual = hoje.replace(day=1)
    fim_mes_anterior = inicio_mes_atual - timedelta(days=1)
    return fim_mes_anterior.strftime("%Y%m")

def mes_anterior_str_range() -> Tuple[str, str]:
    hoje = hoje_ro()
    inicio_mes_atual = hoje.replace(day=1)
    fim_mes_anterior = inicio_mes_atual - timedelta(days=1)
    inicio_mes_anterior = fim_mes_anterior.replace(day=1)
    return (
        inicio_mes_anterior.strftime("%d/%m/%Y"),
        fim_mes_anterior.strftime("%d/%m/%Y"),
    )

def periodo_mes_anterior_str() -> str:
    ini, fim = mes_anterior_str_range()
    return f"{ini} a {fim}"

def mes_anterior_range_dt() -> Tuple[datetime, datetime]:
    hoje = hoje_ro()
    inicio_mes_atual = hoje.replace(day=1)
    fim_mes_anterior = inicio_mes_atual - timedelta(days=1)
    inicio_mes_anterior = fim_mes_anterior.replace(day=1)
    data_ini_dt = datetime(inicio_mes_anterior.year, inicio_mes_anterior.month, inicio_mes_anterior.day, 0, 0, 0)
    data_fim_dt = datetime(fim_mes_anterior.year, fim_mes_anterior.month, fim_mes_anterior.day, 23, 59, 59)
    return data_ini_dt, data_fim_dt


# =========================================================
# PROXY (Render / Datacenter)
# =========================================================
def get_proxies() -> Optional[Dict[str, str]]:
    http_p = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    https_p = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    proxies = {}
    if http_p:
        proxies["http"] = http_p
    if https_p:
        proxies["https"] = https_p
    return proxies or None


# =========================================================
# HELPERS
# =========================================================
def somente_numeros(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\D+", "", str(s))

def norm_text(v: Any) -> str:
    if v is None:
        return ""
    return re.sub(r"\s+", " ", str(v).strip())

def fazer_esta_nao(v: Any) -> bool:
    return norm_text(v).lower() == "nao"

def is_vencido(venc: Any) -> bool:
    if not venc:
        return False
    try:
        s = str(venc)[:10]
        y, m, d = s.split("-")
        vdate = date(int(y), int(m), int(d))
        return vdate < hoje_ro()
    except Exception:
        return False

def montar_nome_final_arquivo(
    base_name: str,
    user: str,
    codi: Optional[int],
    mes_cod: str,
    doc: str,
) -> str:
    doc_clean = somente_numeros(doc) or "sem-doc"
    cod_str = str(codi) if codi is not None else "0"
    email = (user or "sem-user").replace("/", "_")
    return f"{mes_cod}-{cod_str}-{doc_clean}-{email}-{base_name}"


# =========================================================
# SUPABASE: CERTIFICADOS
# =========================================================
def carregar_certificados_validos() -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{TABELA_CERTS}"
    params = {"select": 'id,pem,key,empresa,codi,user,vencimento,"cnpj/cpf",fazer'}
    print("🔎 Buscando certificados na tabela certifica_dfe (REST Supabase)...")
    r = requests.get(url, headers=supabase_headers(), params=params, timeout=30)
    r.raise_for_status()
    certs = r.json() or []
    print(f"   ✔ {len(certs)} certificados encontrados.")
    return certs

def criar_arquivos_cert_temp(cert_row: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    Cria um diretório temporário e grava cert/key.
    Retorna: (cert_path, key_path, tmp_dir)
    """
    pem_b64 = cert_row.get("pem") or ""
    key_b64 = cert_row.get("key") or ""

    pem_bytes = base64.b64decode(pem_b64)
    key_bytes = base64.b64decode(key_b64)

    tmp_dir = tempfile.mkdtemp(prefix="cert_")
    cert_path = os.path.join(tmp_dir, "cert.pem")
    key_path  = os.path.join(tmp_dir, "key.pem")

    with open(cert_path, "wb") as f:
        f.write(pem_bytes)
    with open(key_path, "wb") as f:
        f.write(key_bytes)

    print(f"   ✔ Cert temporário: {cert_path}")
    return cert_path, key_path, tmp_dir


# =========================================================
# SUPABASE: STORAGE
# =========================================================
def arquivo_ja_existe_no_storage(storage_path: str) -> bool:
    storage_path = storage_path.lstrip("/")
    pasta = os.path.dirname(storage_path).replace("\\", "/")
    arquivo = os.path.basename(storage_path)

    url = f"{SUPABASE_URL}/storage/v1/object/list/{BUCKET_IMAGENS}"
    headers = supabase_headers(is_json=True)

    payload = {
        "prefix": pasta,
        "search": arquivo,
        "limit": 100,
        "offset": 0,
        "sortBy": {"column": "name", "order": "asc"},
    }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        if r.status_code != 200:
            print(f"   ⚠️ LIST retornou {r.status_code} ao checar {storage_path}: {r.text[:200]}")
            return False

        itens = r.json() or []
        existe = any((i.get("name") == arquivo) for i in itens)
        if existe:
            print(f"   ⚠️ Já existe no storage: {storage_path}")
        return existe

    except Exception as e:
        print(f"   ⚠️ Erro ao checar existência no storage (LIST) ({storage_path}): {e}")
        return False

def upload_para_storage(storage_path: str, conteudo: bytes, content_type: str = "application/zip") -> bool:
    storage_path = storage_path.lstrip("/")
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_IMAGENS}/{storage_path}"
    headers = supabase_headers()
    headers["Content-Type"] = content_type

    try:
        r = requests.post(url, headers=headers, data=conteudo, timeout=180)
        if r.status_code in (200, 201):
            print(f"   🎉 Upload OK: {storage_path}")
            return True
        print(f"   ❌ Upload erro ({r.status_code}) {storage_path}: {r.text[:400]}")
        return False
    except Exception as e:
        print(f"   ❌ Erro upload ({storage_path}): {e}")
        return False


# =========================================================
# =========================================================
# ============ 1) DF-e RO (SEFIN/RO) =======================
# =========================================================
# =========================================================

ANTI_CAPTCHA_KEY = os.getenv("ANTI_CAPTCHA_KEY", "60ce5191cf427863d4f3c79ee20e4afe").strip()

URL_BASE  = "https://download.dfe.sefin.ro.gov.br"
URL_NOVO  = URL_BASE + "/solicitacoes/novo"
URL_SOLICITACOES       = URL_BASE + "/solicitacoes"
URL_DETALHES_TEMPLATE  = URL_BASE + "/solicitacoes/detalhes/{id}"
URL_CREATE_BASE        = URL_BASE

DFE_TYPES_MAP = {
    "NFe": "0",
    "CTe": "1",
    "NFCe": "2",
}

TIPO_SOLICITACAO = "1"  # 1=PERIODO
MAX_TENTATIVAS = 5
DELAY_ENTRE_TENTATIVAS = 2

def diagnostico_rede_anticaptcha():
    host = "api.anti-captcha.com"
    proxies = get_proxies()
    print("\n[DIAG] Anti-Captcha: diagnóstico rápido...")
    print(f"[DIAG] Usando proxies? {'SIM' if proxies else 'NÃO'}")
    try:
        ip = socket.gethostbyname(host)
        print(f"[DIAG] DNS OK: {host} -> {ip}")
    except Exception as e:
        print(f"[DIAG] DNS FALHOU para {host}: {e}")
        return
    try:
        r = requests.get(f"https://{host}", timeout=(15, 20), proxies=proxies)
        print(f"[DIAG] GET https://{host} -> status {r.status_code}")
    except Exception as e:
        print(f"[DIAG] GET https://{host} falhou/timeout: {e}")

def normalizar_tipo_documento(texto: str) -> Optional[str]:
    if not texto:
        return None
    t = re.sub(r"\s+", " ", texto.strip().upper())
    if "CTE" in t or "CT-E" in t or "CONHECIMENTO" in t:
        return "CTe"
    if "NFCE" in t or "NFC-E" in t or "CONSUMIDOR" in t:
        return "NFCe"
    if "NFE" in t or "NF-E" in t or "NOTA FISCAL" in t:
        if "NFC" in t:
            return "NFCe"
        return "NFe"
    return None

def criar_sessao_dfe_ro(cert_path: str, key_path: str) -> requests.Session:
    s = requests.Session()
    s.cert = (cert_path, key_path)
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
    print("✅ (DFE-RO) Certificado e chave carregados.")
    return s

# Anti-captcha session robusta
_ANTI_SESSION = requests.Session()
_anti_retries = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["POST"]),
    raise_on_status=False,
)
_ANTI_SESSION.mount("https://", HTTPAdapter(max_retries=_anti_retries))

def resolver_captcha_anticaptcha(b64_image_content: str) -> Optional[str]:
    if not ANTI_CAPTCHA_KEY:
        print("⚠️ ANTI_CAPTCHA_KEY vazia. Sem captcha automático.")
        return None

    proxies = get_proxies()
    print("🤖 (DFE-RO) Anti-Captcha...")
    start_time = time.time()

    create_payload: Dict[str, Any] = {
        "clientKey": ANTI_CAPTCHA_KEY,
        "task": {
            "type": "ImageToTextTask",
            "body": b64_image_content,
            "phrase": False,
            "case": True,
            "numeric": 0,
        },
    }

    CONNECT_TIMEOUT = 45
    READ_TIMEOUT = 120

    try:
        r = _ANTI_SESSION.post(
            "https://api.anti-captcha.com/createTask",
            json=create_payload,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            proxies=proxies,
        )
        r.raise_for_status()
        resp = r.json()
        task_id = resp.get("taskId")
        if not task_id:
            print("❌ Anti-Captcha createTask sem taskId:", resp)
            return None

        max_polls = 14
        for i in range(max_polls):
            time.sleep(3)
            try:
                r2 = _ANTI_SESSION.post(
                    "https://api.anti-captcha.com/getTaskResult",
                    json={"clientKey": ANTI_CAPTCHA_KEY, "taskId": task_id},
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                    proxies=proxies,
                )
                r2.raise_for_status()
                result = r2.json()
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Poll Anti-Captcha ({i+1}/{max_polls}) falhou: {e}")
                continue

            status = result.get("status")
            if status == "ready":
                text = (result.get("solution") or {}).get("text")
                if text:
                    print(f"✅ Anti-Captcha OK em {time.time() - start_time:.1f}s: {text}")
                    return text
                print("❌ Anti-Captcha 'ready' sem texto:", result)
                return None

            if status not in ("processing", None):
                print("❌ Anti-Captcha erro:", result)
                return None

        print("❌ Anti-Captcha timeout (polling).")
        return None

    except requests.exceptions.ConnectTimeout:
        print("❌ Anti-Captcha: timeout de CONEXÃO.")
        return None
    except requests.exceptions.ReadTimeout:
        print("❌ Anti-Captcha: timeout de RESPOSTA.")
        return None
    except requests.exceptions.RequestException as e:
        print("❌ Anti-Captcha: erro HTTP/rede:", e)
        return None
    except Exception as e:
        print("❌ Anti-Captcha: erro inesperado:", e)
        return None

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

    if not csrf_token or not token_captcha or not cnpj_limpo:
        raise Exception("Erro na extração dos tokens de segurança (CSRF, Token, CNPJ).")

    print(f"   ✅ Tokens extraídos. CNPJ: {cnpj_limpo}")
    return csrf_token, token_captcha, cnpj_limpo, URL_CREATE, img_bytes, b64

def enviar_solicitacao_unica(s: requests.Session, dfe_name: str, dfe_type_code: str) -> bool:
    print("\n========================================================")
    print(f"🚀 (DFE-RO) SOLICITAÇÃO: {dfe_name} (Tipo: {dfe_type_code})")
    print("========================================================")

    r_novo = s.get(URL_NOVO, timeout=30, allow_redirects=True)
    if r_novo.status_code != 200:
        print(f"   ERRO: Status {r_novo.status_code} ao abrir /novo")
        return False

    try:
        csrf_token, token_captcha, cnpj_limpo, URL_CREATE, _img_bytes, b64_captcha = extrair_tokens_e_captcha(r_novo.text)
    except Exception as e:
        print(f"❌ Erro extraindo tokens/captcha: {e}")
        return False

    captcha_resposta: Optional[str] = resolver_captcha_anticaptcha(b64_captcha)
    if not captcha_resposta:
        print("❌ Sem captcha automático. Abortando solicitação.")
        return False

    data_ini, data_fim = mes_anterior_str_range()

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

    headers: Dict[str, str] = {
        "Referer": URL_NOVO,
        "X-CSRF-Token": csrf_token,
        "X-Requested-With": "XMLHttpRequest",
    }

    r_post = s.post(URL_CREATE, data=payload, headers=headers, timeout=60, allow_redirects=False)
    print(f"   POST status: {r_post.status_code}")

    if r_post.status_code == 302:
        print("🎉 SUCESSO (302).")
        return True

    if r_post.status_code == 200:
        response_text = r_post.text.strip()
        if response_text == '{"status":"Texto de verificação inválido"}':
            print(f"❌ CAPTCHA inválido ({dfe_name}).")
            return False
        if '"status":"ok"' in response_text or '"status":"success"' in response_text:
            print(f"✅ Solicitação {dfe_name} aceita.")
            return True
        print(f"🛑 Resposta 200 inesperada: {response_text[:300]}")
        return False

    print(f"🛑 Status inesperado: {r_post.status_code}")
    return False

def enviar_solicitacao_sequencial(s: requests.Session, apenas_tipos: Optional[List[str]] = None):
    tipos = list(DFE_TYPES_MAP.items()) if apenas_tipos is None else [(t, DFE_TYPES_MAP[t]) for t in apenas_tipos if t in DFE_TYPES_MAP]
    print("\n=== (DFE-RO) ABRINDO SOLICITAÇÕES (MÊS ANTERIOR) ===")
    for dfe_name, dfe_type_code in tipos:
        tentativas = 0
        success = False
        while tentativas < MAX_TENTATIVAS and not success:
            if tentativas > 0:
                print(f"\n--- Tentativa {tentativas + 1}/{MAX_TENTATIVAS} ({dfe_name}) ---")
                time.sleep(DELAY_ENTRE_TENTATIVAS)
            success = enviar_solicitacao_unica(s, dfe_name, dfe_type_code)
            tentativas += 1

        if not success:
            print(f"[DFE-RO] {dfe_name} falhou após {MAX_TENTATIVAS} tentativas.")
        else:
            print(f"[DFE-RO] {dfe_name} solicitado.")
            time.sleep(DELAY_ENTRE_TENTATIVAS)

def listar_solicitacoes(s: requests.Session) -> List[Dict[str, str]]:
    print("🔎 (DFE-RO) Listando solicitações...")
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
                "documento": tipo_documento,
                "estado": estado,
                "data": data_full,
                "file_name": f"{tipo_documento}_{solicitacao_id}.zip".replace(" ", "_").replace("/", "-"),
            })

    print(f"   ✔ {len(itens)} itens na tabela.")
    return itens

def extrair_detalhes_solicitacao(s: requests.Session, solicitacao_id: str) -> Dict[str, Optional[str]]:
    url = URL_DETALHES_TEMPLATE.format(id=solicitacao_id)
    r = s.get(url, timeout=30)
    if r.status_code != 200:
        return {"periodo": None, "doc": None}

    soup = BeautifulSoup(r.text, "lxml")
    tabela = soup.find("table", class_=re.compile("table-xxs"))
    if not tabela:
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

def _parse_id_num(solicitacao_id: str) -> int:
    try:
        return int(re.sub(r"\D+", "", solicitacao_id or "") or "0")
    except Exception:
        return 0

def _peso_estado(estado: str) -> int:
    e = (estado or "").upper()
    if "DOWNLOAD" in e:
        return 100
    if "GERANDO" in e:
        return 80
    if "PROCESS" in e:
        return 60
    if "FINALIZ" in e or "FINAL" in e:
        return 50
    if "ERRO" in e or "FALH" in e:
        return 10
    return 30

def selecionar_uma_por_tipo(solicitacoes_filtradas: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    melhor: Dict[str, Dict[str, Any]] = {}
    for it in solicitacoes_filtradas:
        tipo = it.get("tipo_norm")
        if not tipo:
            continue
        score = (_peso_estado(it.get("estado", "")), _parse_id_num(it.get("id")))
        cur = melhor.get(tipo)
        if not cur:
            it["_score"] = score
            melhor[tipo] = it
            continue
        if score > cur.get("_score", (0, 0)):
            it["_score"] = score
            melhor[tipo] = it

    for k in list(melhor.keys()):
        melhor[k].pop("_score", None)
    return melhor

def obter_url_captcha(s: requests.Session, solicitacao_id: str) -> Optional[Tuple[str, str]]:
    detalhes_url = URL_DETALHES_TEMPLATE.format(id=solicitacao_id)
    r = s.get(detalhes_url, timeout=30)
    if r.status_code != 200:
        print("❌ Erro ao carregar Detalhes:", r.status_code)
        return None

    soup = BeautifulSoup(r.text, "lxml")
    link = soup.find("a", class_=re.compile(r"\blink-detalhe\b"), href=re.compile(r"get_captcha_download"))
    if not link or not link.has_attr("href"):
        print("❌ Não achei o link do download (get_captcha_download) na página de Detalhes.")
        return None

    href = link["href"]
    captcha_url = URL_BASE + href if not href.startswith("http") else href
    print(f"   ✅ URL get_captcha_download: {captcha_url}")
    return captcha_url, detalhes_url

def extrair_html_modal(js: str) -> Optional[str]:
    m = re.search(r'\$\("#bloco_modal"\)\.html\((?P<q>[\'"])(.+?)(?P=q)\)', js, re.S)
    if m:
        raw = m.group(2)
        return (
            raw.replace("\\n", "\n")
               .replace("\\t", "\t")
               .replace('\\"', '"')
               .replace("\\/", "/")
        )
    if "<form" in js and "captcha_resposta" in js:
        return js
    return None

def realizar_download_dfe(s: requests.Session, solicitacao_data: Dict[str, Any], storage_path: str) -> bool:
    solicitacao_id = solicitacao_data["id"]
    print("\n⬇️ (DFE-RO) Baixando ID", solicitacao_id)

    res = obter_url_captcha(s, solicitacao_id)
    if not res:
        return False
    captcha_url, detalhes_url = res

    r_get = s.get(
        captcha_url,
        headers={"Referer": detalhes_url, "X-Requested-With": "XMLHttpRequest", "Accept": "text/html,*/*"},
        timeout=30,
    )
    if r_get.status_code != 200:
        print("❌ Erro HTTP no get_captcha_download:", r_get.status_code)
        return False

    html_modal = extrair_html_modal(r_get.text)
    if not html_modal:
        print("❌ Não consegui extrair HTML do modal.")
        return False

    soup = BeautifulSoup(html_modal, "lxml")
    form = soup.find("form")
    token_input = form.find("input", {"name": "token"}) if form else None
    img_tag = form.find("img") if form else None

    if not form or not token_input or not img_tag or not img_tag.get("src", "").startswith("data:image"):
        print("❌ Modal sem form/token/img.")
        return False

    action = form.get("action")
    if not action.startswith("http"):
        action = URL_BASE + action

    token = token_input["value"]
    b64 = img_tag["src"].split(",", 1)[1]

    captcha = resolver_captcha_anticaptcha(b64)
    if not captcha:
        print("❌ Sem captcha automático. Abortando download.")
        return False

    params = {"token": token, "captcha_resposta": captcha}
    r_final = s.get(action, params=params, stream=True, timeout=120)

    content_type = (r_final.headers.get("Content-Type") or "").lower()
    if "application/zip" in content_type or "application/octet-stream" in content_type:
        conteudo = b"".join(chunk for chunk in r_final.iter_content(8192) if chunk)
        return upload_para_storage(storage_path, conteudo, content_type="application/zip")

    print("❌ Falha no GET final. Content-Type:", content_type, "| Status:", r_final.status_code)
    return False

def fluxo_dfe_ro_para_empresa(cert_row: Dict[str, Any]):
    empresa = cert_row.get("empresa") or ""
    user = cert_row.get("user") or ""
    codi = cert_row.get("codi")
    venc = cert_row.get("vencimento")
    doc_raw = cert_row.get("cnpj/cpf") or ""
    doc_alvo = somente_numeros(doc_raw) or ""

    print("\n\n========================================================")
    print(f"🏢 (DFE-RO) empresa: {empresa} | user: {user} | codi: {codi} | doc: {doc_raw} | venc: {venc}")
    print("========================================================")

    cert_path = key_path = tmp_dir = None
    try:
        cert_path, key_path, tmp_dir = criar_arquivos_cert_temp(cert_row)
        s = criar_sessao_dfe_ro(cert_path, key_path)
    except Exception as e:
        print("❌ (DFE-RO) Erro criando sessão:", e)
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return

    try:
        solicitacoes = listar_solicitacoes(s)
        periodo_alvo = periodo_mes_anterior_str()
        mes_cod = mes_anterior_codigo()

        filtradas: List[Dict[str, Any]] = []
        for item in solicitacoes:
            solicitacao_id = item["id"]
            estado = (item.get("estado") or "").upper()

            tipo_norm = normalizar_tipo_documento(item.get("documento", ""))
            if not tipo_norm or tipo_norm not in DFE_TYPES_MAP:
                continue

            det = extrair_detalhes_solicitacao(s, solicitacao_id)
            periodo = (det.get("periodo") or "").strip()
            doc_det = somente_numeros(det.get("doc")) if det.get("doc") else ""

            if not periodo or periodo != periodo_alvo:
                continue
            if doc_alvo and doc_det and doc_det != doc_alvo:
                continue

            filtradas.append({
                **item,
                "tipo_norm": tipo_norm,
                "periodo": periodo,
                "doc_det": doc_det,
                "estado": estado,
            })

        escolhidas = selecionar_uma_por_tipo(filtradas)

        if not escolhidas:
            print("⚠️ (DFE-RO) Nenhuma solicitação do mês anterior encontrada.")
        else:
            for tipo, it in escolhidas.items():
                print(f"⭐ (DFE-RO) {tipo}: ID {it['id']} | estado: {it['estado']} | período: {it['periodo']}")

        # baixar somente quando DOWNLOAD
        for tipo in ["CTe", "NFCe", "NFe"]:
            it = escolhidas.get(tipo)
            if not it:
                continue
            if it["estado"] != "DOWNLOAD":
                print(f"   🔄 (DFE-RO) {tipo}: ID {it['id']} está '{it['estado']}'. Não baixa agora.")
                continue

            base_name = it["file_name"]
            nome_final = montar_nome_final_arquivo(
                base_name=base_name,
                user=user,
                codi=codi,
                mes_cod=mes_cod,
                doc=doc_alvo or doc_raw,
            )
            storage_path = f"{PASTA_NOTAS}/{nome_final}"

            if arquivo_ja_existe_no_storage(storage_path):
                print(f"   ⤵ (DFE-RO) {tipo}: já existe no storage: {storage_path}")
                continue

            ok = False
            tent = 0
            while tent < 3 and not ok:
                ok = realizar_download_dfe(s, it, storage_path)
                tent += 1
                if not ok:
                    print(f"   (DFE-RO) {tipo}: tentativa {tent} falhou (ID {it['id']}).")
                    time.sleep(10)

            if ok:
                print(f"   ✅ (DFE-RO) {tipo}: download OK.")
            else:
                print(f"❌ (DFE-RO) {tipo}: falhou após 3 tentativas.")

        # se faltou algum tipo, abre solicitação só do que falta
        faltando = [t for t in DFE_TYPES_MAP.keys() if t not in escolhidas]
        if not faltando:
            print("✅ (DFE-RO) Já existe solicitação do mês anterior para todos os tipos.")
        else:
            print("⚠️ (DFE-RO) Faltam tipos:", ", ".join(faltando))
            print("➡️ (DFE-RO) Abrindo novas solicitações SOMENTE dos tipos faltantes...")
            enviar_solicitacao_sequencial(s, apenas_tipos=faltando)

    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


# =========================================================
# =========================================================
# ============ 2) NFS-e ADN ================================
# =========================================================
# =========================================================
ADN_BASE = "https://adn.nfse.gov.br"

def diagnostico_rede_adn():
    host = "adn.nfse.gov.br"
    print("\n[DIAG] ADN: diagnóstico rápido...")
    try:
        ip = socket.gethostbyname(host)
        print(f"[DIAG] DNS OK: {host} -> {ip}")
    except Exception as e:
        print(f"[DIAG] DNS FALHOU: {e}")
        return
    try:
        r = requests.get(f"https://{host}", timeout=(10, 20))
        print(f"[DIAG] GET https://{host} -> {r.status_code}")
    except Exception as e:
        print(f"[DIAG] GET https://{host} falhou: {e}")

def decode_xml_field(value: str) -> Optional[str]:
    if not isinstance(value, str) or not value:
        return None
    if value.lstrip().startswith("<"):
        return value
    try:
        b = base64.b64decode(value, validate=False)
    except Exception:
        return None
    try:
        return gzip.decompress(b).decode("utf-8", errors="replace")
    except Exception:
        try:
            return b.decode("utf-8", errors="replace")
        except Exception:
            return None

def find_xmls(data: Any) -> List[str]:
    xmls: List[str] = []
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, str):
                xml = decode_xml_field(v)
                if xml and xml.strip().startswith("<"):
                    xmls.append(xml)
            else:
                xmls.extend(find_xmls(v))
    elif isinstance(data, list):
        for item in data:
            xmls.extend(find_xmls(item))
    return xmls

def parse_possible_date(texto: str) -> Optional[datetime]:
    if not texto:
        return None
    t = texto.strip()

    if len(t) >= 10 and t[4:5] == "-" and t[7:8] == "-":
        try:
            return datetime.strptime(t[:10], "%Y-%m-%d")
        except Exception:
            pass

    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(t, fmt)
        except Exception:
            pass

    return None

def xml_in_period(xml_str: str, data_ini: datetime, data_fim: datetime) -> bool:
    try:
        root = etree.fromstring(xml_str.encode("utf-8", errors="ignore"))
        nodes = root.xpath(
            "//*[contains(translate(local-name(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'data') "
            "or contains(translate(local-name(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'compet')]"
        )
        for n in nodes:
            dt = parse_possible_date((n.text or "").strip())
            if dt and data_ini <= dt <= data_fim:
                return True
    except Exception:
        pass
    return False

def criar_sessao_adn(cert_path: str, key_path: str) -> requests.Session:
    s = requests.Session()
    s.cert = (cert_path, key_path)
    s.verify = True
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    })

    retries = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def zipar_pasta_em_memoria(pasta_local: str) -> bytes:
    buf = tempfile.SpooledTemporaryFile(max_size=50 * 1024 * 1024)
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _dirs, files in os.walk(pasta_local):
            for fn in files:
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, pasta_local).replace("\\", "/")
                z.write(full, rel)
    buf.seek(0)
    return buf.read()

def baixar_nfse_mes_anterior_para_pasta(
    s: requests.Session,
    cnpj: str,
    pasta_saida: str,
    start_nsu: int,
    max_nsu: int,
) -> Tuple[int, int]:
    data_ini, data_fim = mes_anterior_range_dt()

    nsu = int(start_nsu)
    limite = int(start_nsu) + int(max_nsu)

    total_salvos = 0
    total_nsu_proc = 0

    while nsu < limite:
        url = f"{ADN_BASE}/contribuintes/DFe/{nsu}?cnpjConsulta={cnpj}"

        try:
            r = s.get(url, timeout=60)
        except Exception as e:
            print(f"[ADN NSU {nsu}] ERRO REDE: {e}")
            nsu += 1
            continue

        if r.status_code == 204:
            print(f"[ADN NSU {nsu}] Sem conteúdo (204). Encerrando.")
            break

        ctype = (r.headers.get("Content-Type") or "").lower()

        if r.status_code >= 400:
            print(f"[ADN NSU {nsu}] HTTP {r.status_code} | CT={ctype} | Corpo: {str(r.text)[:200]}")
            nsu += 1
            continue

        if "application/json" not in ctype:
            print(f"[ADN NSU {nsu}] Não-JSON. CT={ctype} | Corpo: {str(r.text)[:200]}")
            nsu += 1
            continue

        try:
            data = r.json()
        except Exception as e:
            print(f"[ADN NSU {nsu}] JSON inválido ({e}).")
            nsu += 1
            continue

        total_nsu_proc += 1

        # salva bruto
        raw_fn = os.path.join(pasta_saida, f"nsu_{nsu}_raw.json")
        try:
            with open(raw_fn, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[ADN NSU {nsu}] Falha ao salvar JSON bruto: {e}")

        xmls = find_xmls(data)
        salvos_nsu = 0

        for i, xml in enumerate(xmls, start=1):
            if xml_in_period(xml, data_ini, data_fim):
                total_salvos += 1
                salvos_nsu += 1
                nome = f"NFS-e_{nsu}_{i}_{total_salvos}.xml"
                xml_path = os.path.join(pasta_saida, nome)
                try:
                    with open(xml_path, "w", encoding="utf-8") as f:
                        f.write(xml)
                except Exception as e:
                    print(f"[ADN NSU {nsu}] Erro salvando XML {nome}: {e}")

        print(f"[ADN NSU {nsu}] OK - XMLs: {len(xmls)} | salvos no período: {salvos_nsu}")
        nsu += 1

    return total_salvos, total_nsu_proc

def fluxo_nfse_para_empresa(cert_row: Dict[str, Any]):
    empresa = cert_row.get("empresa") or ""
    user = cert_row.get("user") or ""
    codi = cert_row.get("codi")
    venc = cert_row.get("vencimento")
    doc_raw = cert_row.get("cnpj/cpf") or ""
    doc_alvo = somente_numeros(doc_raw) or ""

    print("\n\n========================================================")
    print(f"🏢 (NFS-e ADN) empresa: {empresa} | user: {user} | codi: {codi} | doc: {doc_raw} | venc: {venc}")
    print("========================================================")

    if not doc_alvo or len(doc_alvo) < 11:
        print("⏭️ (NFS-e) Pulando: doc inválido/ausente.")
        return

    cert_path = key_path = tmp_dir = None
    work_dir = None
    try:
        cert_path, key_path, tmp_dir = criar_arquivos_cert_temp(cert_row)
        s = criar_sessao_adn(cert_path, key_path)

        work_dir = tempfile.mkdtemp(prefix="nfse_adn_")
        print(f"   📁 (NFS-e) Pasta temp: {work_dir}")

        total_xml, total_nsu = baixar_nfse_mes_anterior_para_pasta(
            s=s,
            cnpj=doc_alvo,
            pasta_saida=work_dir,
            start_nsu=START_NSU_DEFAULT,
            max_nsu=MAX_NSU_DEFAULT,
        )

        if total_nsu == 0:
            print("⚠️ (NFS-e) Nada processado no ADN.")
            return

        tem_arquivos = any(files for _root, _dirs, files in os.walk(work_dir))
        if not tem_arquivos:
            print("⚠️ (NFS-e) Pasta vazia. Não envia ZIP.")
            return

        mes_cod = mes_anterior_codigo()
        base_name = f"NFSE_{mes_cod}.zip"
        nome_final = montar_nome_final_arquivo(
            base_name=base_name,
            user=user,
            codi=codi,
            mes_cod=mes_cod,
            doc=doc_alvo or doc_raw,
        )
        storage_path = f"{PASTA_NOTAS}/{nome_final}"

        if arquivo_ja_existe_no_storage(storage_path):
            print(f"⤵ (NFS-e) Já existe no storage. Não reenvia: {storage_path}")
            return

        zip_bytes = zipar_pasta_em_memoria(work_dir)
        print(f"   📦 (NFS-e) ZIP pronto ({len(zip_bytes)/1024:.1f} KB) | XMLs período: {total_xml} | NSUs: {total_nsu}")

        ok = upload_para_storage(storage_path, zip_bytes, content_type="application/zip")
        if ok:
            print(f"✅ (NFS-e) Enviado: {storage_path}")
        else:
            print(f"❌ (NFS-e) Falhou upload: {storage_path}")

    except Exception as e:
        print(f"❌ (NFS-e) Erro inesperado: {e}")
    finally:
        if work_dir:
            shutil.rmtree(work_dir, ignore_errors=True)
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


# =========================================================
# LOOP ÚNICO: PROCESSA AMBOS POR EMPRESA
# =========================================================
def processar_todas_empresas():
    certs = carregar_certificados_validos()
    if not certs:
        print("⚠️ Nenhum certificado encontrado.")
        return

    hoje = hoje_ro()

    for cert_row in certs:
        empresa = cert_row.get("empresa") or "(sem empresa)"
        user = cert_row.get("user") or ""
        venc = cert_row.get("vencimento")
        fazer = cert_row.get("fazer")

        if fazer_esta_nao(fazer):
            print(f"\n⏭️ PULANDO (fazer='nao'): {empresa} | user: {user}")
            continue

        if is_vencido(venc):
            print(f"\n⏭️ PULANDO (CERT VENCIDO): {empresa} | user: {user} | venc: {venc} | hoje: {hoje.isoformat()}")
            continue

        # roda DF-e RO
        if RUN_DFE:
            try:
                fluxo_dfe_ro_para_empresa(cert_row)
            except Exception as e:
                print(f"💥 (DFE-RO) Erro inesperado em {empresa}: {e}")

        # roda NFS-e ADN
        if RUN_NFSE:
            try:
                fluxo_nfse_para_empresa(cert_row)
            except Exception as e:
                print(f"💥 (NFS-e) Erro inesperado em {empresa}: {e}")


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    print("========================================================")
    print("ROBÔ ÚNICO: DFE-RO + NFS-e ADN")
    print(f"RUN_DFE={int(RUN_DFE)} | RUN_NFSE={int(RUN_NFSE)}")
    print(f"Fuso: {FUSO_RO} | Hoje: {hoje_ro().strftime('%d/%m/%Y')}")
    print(f"Loop a cada {INTERVALO_LOOP_SEGUNDOS}s")
    print("========================================================")

    # diagnósticos
    if RUN_DFE:
        diagnostico_rede_anticaptcha()
    if RUN_NFSE:
        diagnostico_rede_adn()

    while True:
        print("\n\n==================== NOVA VARREDURA GERAL ====================")
        print(f"📅 Data (fuso RO): {hoje_ro().strftime('%d/%m/%Y')}")
        try:
            processar_todas_empresas()
        except Exception as e:
            print(f"💥 Erro inesperado no loop principal: {e}")

        print(f"🕒 Aguardando {INTERVALO_LOOP_SEGUNDOS} segundos...\n")
        time.sleep(INTERVALO_LOOP_SEGUNDOS)
