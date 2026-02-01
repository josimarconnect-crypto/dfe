# -*- coding: utf-8 -*-
import requests
import time
import re
import os
import base64
import tempfile
import socket
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
from typing import Dict, Any, Optional, List, Tuple
from zoneinfo import ZoneInfo  # 👈 Fuso horário

# Retry helpers
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

# Você pode manter aqui OU setar no Render (Environment -> ANTI_CAPTCHA_KEY)
ANTI_CAPTCHA_KEY = os.getenv("ANTI_CAPTCHA_KEY", "60ce5191cf427863d4f3c79ee20e4afe").strip()

URL_HOME  = "https://dfe.sefin.ro.gov.br/"
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
DELAY_ENTRE_TENTATIVAS = 1
INTERVALO_LOOP_SEGUNDOS = 36


# =========================================================
# FUSO HORÁRIO (RONDÔNIA)
# =========================================================
FUSO_RO = ZoneInfo("America/Porto_Velho")

def hoje_ro() -> date:
    return datetime.now(FUSO_RO).date()


# =========================================================
# PROXY (Render / Datacenter)
# =========================================================
def get_proxies() -> Optional[Dict[str, str]]:
    """
    Usa variáveis de ambiente padrão:
      HTTP_PROXY / HTTPS_PROXY
    No Render: Settings -> Environment
    """
    http_p = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    https_p = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")

    proxies = {}
    if http_p:
        proxies["http"] = http_p
    if https_p:
        proxies["https"] = https_p

    return proxies or None


# =========================================================
# DIAGNÓSTICO DE REDE (Anti-Captcha)
# =========================================================
def diagnostico_rede_anticaptcha():
    """
    Só para LOGAR e ajudar você a identificar se é:
    - DNS
    - Bloqueio/rota
    - Lentidão
    Não quebra o robô.
    """
    host = "api.anti-captcha.com"
    proxies = get_proxies()

    print("\n[DIAG] Anti-Captcha: iniciando diagnóstico rápido de rede...")
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


# =========================================================
# FUNÇÕES AUXILIARES
# =========================================================
def somente_numeros(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\D+", "", s)

def mes_anterior_codigo() -> str:
    hoje = hoje_ro()
    inicio_mes_atual = hoje.replace(day=1)
    fim_mes_anterior = inicio_mes_atual - timedelta(days=1)
    return fim_mes_anterior.strftime("%Y%m")

def mes_anterior() -> Tuple[str, str]:
    hoje = hoje_ro()
    inicio_mes_atual = hoje.replace(day=1)
    fim_mes_anterior = inicio_mes_atual - timedelta(days=1)
    inicio_mes_anterior = fim_mes_anterior.replace(day=1)
    return (
        inicio_mes_anterior.strftime("%d/%m/%Y"),
        fim_mes_anterior.strftime("%d/%m/%Y"),
    )

def periodo_mes_anterior_str() -> str:
    ini, fim = mes_anterior()
    return f"{ini} a {fim}"

def normalizar_tipo_documento(texto: str) -> Optional[str]:
    """
    Normaliza a coluna 'DOCUMENTO' da listagem para: NFe / CTe / NFCe
    """
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

def norm_text(v: Any) -> str:
    if v is None:
        return ""
    return re.sub(r"\s+", " ", str(v).strip())

def fazer_esta_nao(v: Any) -> bool:
    """
    Retorna True se a coluna 'fazer' estiver como 'nao' (case-insensitive).
    Aceita variações com espaços.
    """
    t = norm_text(v).lower()
    return t == "nao"

def is_vencido(venc: Any) -> bool:
    """
    Vencimento vem como 'YYYY-MM-DD' (string) pelo REST do Supabase.
    Considera vencido se vencimento < hoje_ro().
    Se vencimento estiver vazio/nulo, considera NÃO vencido.
    """
    if not venc:
        return False
    try:
        s = str(venc)[:10]
        y, m, d = s.split("-")
        vdate = date(int(y), int(m), int(d))
        return vdate < hoje_ro()
    except Exception:
        return False


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

def criar_arquivos_cert_temp(cert_row: Dict[str, Any]) -> Tuple[str, str]:
    pem_b64 = cert_row.get("pem") or ""
    key_b64 = cert_row.get("key") or ""

    pem_bytes = base64.b64decode(pem_b64)
    key_bytes = base64.b64decode(key_b64)

    cert_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    key_file  = tempfile.NamedTemporaryFile(delete=False, suffix=".key")

    cert_file.write(pem_bytes); cert_file.flush(); cert_file.close()
    key_file.write(key_bytes);  key_file.flush();  key_file.close()

    print(f"   ✔ Arquivos temporários de certificado criados: {cert_file.name}, {key_file.name}")
    return cert_file.name, key_file.name


# =========================================================
# SUPABASE: STORAGE (CHECAGEM POR LIST = ROBUSTA)
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
            print(f"   ⚠️ Arquivo já existente no storage: {storage_path}")
            return True
        return False

    except Exception as e:
        print(f"   ⚠️ Erro ao checar existência no storage (LIST) ({storage_path}): {e}")
        return False


def upload_para_storage(storage_path: str, conteudo: bytes, content_type: str = "application/zip") -> bool:
    storage_path = storage_path.lstrip("/")
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_IMAGENS}/{storage_path}"
    headers = supabase_headers()
    headers["Content-Type"] = content_type

    try:
        r = requests.post(url, headers=headers, data=conteudo, timeout=120)
        if r.status_code in (200, 201):
            print(f"   🎉 Upload realizado para Supabase: {storage_path}")
            return True
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
    doc_clean = somente_numeros(doc) or "sem-doc"
    cod_str = str(codi) if codi is not None else "0"
    email = user or "sem-user"
    return f"{mes_cod}-{cod_str}-{doc_clean}-{email}-{base_name}"


# =========================================================
# SESSÃO mTLS
# =========================================================
def criar_sessao(cert_path: str, key_path: str) -> requests.Session:
    s = requests.Session()
    s.cert = (cert_path, key_path)
    print("✅ Certificado e chave carregados com sucesso.")

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
# ANTI-CAPTCHA (ROBUSTO PARA RENDER)
# =========================================================
_ANTI_SESSION = requests.Session()
_retries = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["POST"]),
    raise_on_status=False,
)
_ANTI_SESSION.mount("https://", HTTPAdapter(max_retries=_retries))


def resolver_captcha_anticaptcha(b64_image_content: str) -> Optional[str]:
    if not ANTI_CAPTCHA_KEY:
        print("⚠️ ANTI_CAPTCHA_KEY vazia. Usando modo manual.")
        return None

    proxies = get_proxies()

    print("🤖 Tentando Anti-Captcha API...")
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

    # Render costuma precisar de timeouts maiores
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

        max_polls = 14  # ~ 14*3s = 42s
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
                print(f"⚠️ Falha no polling Anti-Captcha ({i+1}/{max_polls}): {e}")
                continue

            status = result.get("status")
            if status == "ready":
                text = (result.get("solution") or {}).get("text")
                if text:
                    print(f"✅ Anti-Captcha resolveu em {time.time() - start_time:.1f}s: {text}")
                    return text
                print("❌ Anti-Captcha 'ready' mas sem texto:", result)
                return None

            if status not in ("processing", None):
                print("❌ Anti-Captcha retornou erro:", result)
                return None

        print("❌ Anti-Captcha não resolveu a tempo (timeout de polling).")
        return None

    except requests.exceptions.ConnectTimeout:
        print("❌ Anti-Captcha: timeout de CONEXÃO (Render/rota/bloqueio). Indo para modo manual.")
        return None
    except requests.exceptions.ReadTimeout:
        print("❌ Anti-Captcha: timeout de RESPOSTA (servidor lento). Indo para modo manual.")
        return None
    except requests.exceptions.RequestException as e:
        print("❌ Anti-Captcha: erro HTTP/rede:", e)
        return None
    except Exception as e:
        print("❌ Anti-Captcha: erro inesperado:", e)
        return None


# =========================================================
# CRIAR SOLICITAÇÕES (MÊS ANTERIOR)
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
        csrf_token, token_captcha, cnpj_limpo, URL_CREATE, _img_bytes, b64_captcha = extrair_tokens_e_captcha(r_novo.text)
    except Exception as e:
        print(f"❌ Erro na extração dos dados: {e}")
        return False

    captcha_resposta: Optional[str] = resolver_captcha_anticaptcha(b64_captcha)
    if not captcha_resposta:
        print("\n====================================================================")
        print("🛑 MODO MANUAL: Resolução automática falhou ou indisponível no Render.")
        print("====================================================================")
        # No Render não tem input() prático; então aborta com False.
        print("❌ Sem captcha automático e sem entrada manual. Abortando esta solicitação.")
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
    print(f"⏱️ TEMPO TOTAL GASTO ANTES DO POST: {(time.time() - start_total_time):.2f} segundos.")

    r_post = s.post(URL_CREATE, data=payload, headers=headers, timeout=60, allow_redirects=False)
    print(f"   Status FINAL do POST: {r_post.status_code}")

    if r_post.status_code == 302:
        print("🎉 SUCESSO COMPLETO (302 REDIRECIONAMENTO).")
        return True

    if r_post.status_code == 200:
        response_text = r_post.text.strip()
        print(f"   Resposta do Servidor (200): {response_text}")
        if response_text == '{"status":"Texto de verificação inválido"}':
            print(f"❌ ERRO CRÍTICO: 'Texto de verificação inválido' ({dfe_name}).")
            return False
        if '"status":"ok"' in response_text or '"status":"success"' in response_text:
            print(f"✅ SUCESSO: Solicitação de {dfe_name} aceita.")
            return True
        print(f"🛑 ERRO DE VALIDAÇÃO (200): verificar conteúdo para {dfe_name}.")
        return False

    print(f"🛑 ERRO INESPERADO: Status {r_post.status_code} em {dfe_name}.")
    return False


def enviar_solicitacao_sequencial(s: requests.Session, apenas_tipos: Optional[List[str]] = None):
    tipos = list(DFE_TYPES_MAP.items()) if apenas_tipos is None else [(t, DFE_TYPES_MAP[t]) for t in apenas_tipos if t in DFE_TYPES_MAP]

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
            time.sleep(DELAY_ENTRE_TENTATIVAS)


# =========================================================
# LISTAR SOLICITAÇÕES
# =========================================================
def listar_solicitacoes(s: requests.Session) -> List[Dict[str, str]]:
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
                "documento": tipo_documento,
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


# =========================================================
# SELEÇÃO: 1 SOLICITAÇÃO POR TIPO (TIPO + PERÍODO)
# =========================================================
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


# =========================================================
# DOWNLOAD (CAPTCHA POPUP)
# =========================================================
def obter_url_captcha(s: requests.Session, solicitacao_id: str) -> Optional[Tuple[str, str]]:
    detalhes_url = URL_DETALHES_TEMPLATE.format(id=solicitacao_id)
    r = s.get(detalhes_url, timeout=30)
    if r.status_code != 200:
        print("❌ Erro ao carregar Detalhes:", r.status_code)
        return None

    soup = BeautifulSoup(r.text, "lxml")
    link = soup.find("a", class_=re.compile(r"\blink-detalhe\b"), href=re.compile(r"get_captcha_download"))
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
        return (
            raw.replace("\\n", "\n")
               .replace("\\t", "\t")
               .replace('\\"', '"')
               .replace("\\/", "/")
        )

    if "<form" in js and "captcha_resposta" in js:
        return js

    print("❌ HTML do modal não encontrado.")
    return None

def realizar_download_dfe(s: requests.Session, solicitacao_data: Dict[str, Any], storage_path: str) -> bool:
    solicitacao_id = solicitacao_data["id"]
    print("\n⬇️ Iniciando download do ID", solicitacao_id)

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
        print("❌ Erro HTTP ao buscar get_captcha_download:", r_get.status_code)
        return False

    html_modal = extrair_html_modal(r_get.text)
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
        print("❌ Sem captcha automático no Render. Abortando download deste ID.")
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
# FLUXO POR EMPRESA
# =========================================================
def fluxo_completo_para_empresa(cert_row: Dict[str, Any]):
    empresa = cert_row.get("empresa") or ""
    user = cert_row.get("user") or ""
    codi = cert_row.get("codi")
    venc = cert_row.get("vencimento")
    doc_raw = cert_row.get("cnpj/cpf") or ""
    doc_alvo = somente_numeros(doc_raw) or ""

    print("\n\n========================================================")
    print(f"🏢 Iniciando fluxo para empresa: {empresa} | user: {user} | codi: {codi} | doc: {doc_raw} | venc: {venc}")
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
        print("⚠️ Nenhuma solicitação encontrada para o período alvo (mês anterior).")
    else:
        for tipo, it in escolhidas.items():
            print(f"⭐ Escolhida para {tipo}: ID {it['id']} | estado: {it['estado']} | período: {it['periodo']} | doc(det): {it.get('doc_det') or 'N/D'}")

    for tipo in ["CTe", "NFCe", "NFe"]:
        it = escolhidas.get(tipo)
        if not it:
            continue

        if it["estado"] != "DOWNLOAD":
            print(f"   🔄 {tipo}: escolhida ID {it['id']} ainda está em '{it['estado']}'. Não baixa agora.")
            continue

        base_name = it["file_name"]
        nome_final = montar_nome_final_arquivo(
            base_name=base_name,
            empresa=empresa,
            user=user,
            codi=codi,
            mes_cod=mes_cod,
            doc=doc_alvo or doc_raw,
        )
        storage_path = f"{PASTA_NOTAS}/{nome_final}"

        if arquivo_ja_existe_no_storage(storage_path):
            print(f"   ⤵ {tipo}: já existe no Supabase, não baixa: {storage_path}")
            continue

        ok = False
        tent = 0
        while tent < 3 and not ok:
            ok = realizar_download_dfe(s, it, storage_path)
            tent += 1
            if not ok:
                print(f"   {tipo}: Tentativa {tent} falhou para ID {it['id']}.")
                time.sleep(10)

        if ok:
            print(f"   ✅ {tipo}: Download concluído (ID {it['id']}).")
        else:
            print(f"❌ {tipo}: Falha crítica ao baixar ID {it['id']} depois de 3 tentativas.")

    faltando = [t for t in DFE_TYPES_MAP.keys() if t not in escolhidas]
    if not faltando:
        print("\n✅ Já existe solicitação do MÊS ANTERIOR para TODOS os tipos (considerando TIPO+PERÍODO).")
        print("   ❌ Não será aberta nova solicitação agora (evita duplicar pedidos).")
    else:
        print("\n⚠️ Faltam solicitaitações do mês anterior nos tipos:", ", ".join(faltando))
        print("➡️ Abrindo novas solicitações SOMENTE para os tipos faltantes...")
        enviar_solicitacao_sequencial(s, apenas_tipos=faltando)


# =========================================================
# MAIN
# =========================================================
def processar_todas_empresas():
    certs = carregar_certificados_validos()
    if not certs:
        print("⚠️ Nenhum certificado encontrado na tabela certifica_dfe.")
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

        try:
            fluxo_completo_para_empresa(cert_row)
        except Exception as e:
            print(f"❌ Erro inesperado ao processar empresa {empresa}: {e}")


if __name__ == "__main__":
    # diagnóstico só uma vez ao iniciar (pra você ver no log do Render)
    diagnostico_rede_anticaptcha()

    while True:
        print("\n\n==================== NOVA VARREDURA GERAL ====================")
        print(f"📅 Data (fuso RO): {hoje_ro().strftime('%d/%m/%Y')}")
        try:
            processar_todas_empresas()
        except Exception as e:
            print(f"💥 Erro inesperado no loop principal: {e}")
        print(f"🕒 Aguardando {INTERVALO_LOOP_SEGUNDOS} segundos para próxima varredura...\n")
        time.sleep(INTERVALO_LOOP_SEGUNDOS)
