import json
import re
import time
import base64
import unicodedata
from io import BytesIO
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime

import pandas as pd
import streamlit as st
from PIL import Image
from pypdf import PdfReader
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage

# =========================================================
# CONFIGURACION GENERAL
# =========================================================
st.set_page_config(page_title="Gestión de Bitácoras", layout="wide")
st.title("📋 Análisis Técnico de Terreno")

MODEL_CANDIDATES = [
    "gemini-3.1-flash-lite-preview",
    "gemini-3-flash-preview",
    "gemini-1.5-flash",
]

ROOT_DRIVE_FOLDER_NAMES = [
    "Bitacoras 2025",
]

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

MAX_ARCHIVOS_EN_POOL = 150
MAX_TEXT_CHARS_POR_ARCHIVO = 12000
MAX_TEXT_CHARS_POR_TANDA = 26000
MAX_ARCHIVOS_POR_TANDA = 6
MAX_PAGINAS_PDF = 8
MAX_FILAS_TABLA = 80
MAX_RECURSION_ITEMS = 400
TOP_RESULTADOS_POR_QUERY = 30
MAX_SHARED_FOLDER_CANDIDATES = 8
MAX_WELL_FOLDER_CANDIDATES = 10
MAX_SHEETS_TABS = 20
MAX_SHEET_ROWS = 200
MAX_SHEET_COLS = 40

STOPWORDS = {
    "dame", "quiero", "necesito", "podrias", "podrías", "puedes",
    "consulta", "pregunta", "sobre", "para", "desde", "hasta", "entre",
    "cuando", "cuándo", "ultima", "última", "ultimo", "último",
    "vez", "registro", "registros", "bitacora", "bitácora",
    "documentos", "archivos", "carpeta", "carpetas", "drive",
    "pozo", "pozos", "que", "qué", "cual", "cuál", "como", "cómo",
    "del", "los", "las", "una", "unos", "unas", "ese", "esa",
    "dia", "días", "día", "dias", "instalados", "instalado",
    "realizaron", "realizado", "labores", "actividad", "actividades",
    "abril", "marzo", "febrero", "enero", "mayo", "junio", "julio",
    "agosto", "septiembre", "setiembre", "octubre", "noviembre", "diciembre",
    "archivo", "acceso", "ver", "leer", "tienes", "puedes", "existe",
}

MESES = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

MESES_NUM_A_NOMBRE = {
    1: "enero",
    2: "febrero",
    3: "marzo",
    4: "abril",
    5: "mayo",
    6: "junio",
    7: "julio",
    8: "agosto",
    9: "septiembre",
    10: "octubre",
    11: "noviembre",
    12: "diciembre",
}

IMAGE_MIME_PREFIX = "image/"
GOOGLE_FOLDER_MIME = "application/vnd.google-apps.folder"
GOOGLE_SHEET_MIME = "application/vnd.google-apps.spreadsheet"

# =========================================================
# ESTADO
# =========================================================
if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])


# =========================================================
# SERVICIOS
# =========================================================
@st.cache_resource
def get_google_services():
    info_claves = json.loads(st.secrets["GOOGLE_JSON_COMPLETO"])
    creds = service_account.Credentials.from_service_account_info(
        info_claves,
        scopes=SCOPES
    )

    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    return drive_service, sheets_service

def debug_log(msg: str):
    with st.expander("Depuración", expanded=False):
        st.write(msg)


def build_llm(model_name: str):
    return ChatGoogleGenerativeAI(
        model=model_name,
        google_api_key=st.secrets["GEMINI_API_KEY"],
        temperature=0,
    )


def get_working_llm():
    last_error = None
    for model_name in MODEL_CANDIDATES:
        try:
            llm = build_llm(model_name)
            prueba = llm.invoke([HumanMessage(content="Responde solo OK")])
            texto = normalizar_respuesta_llm(getattr(prueba, "content", ""))
            if texto:
                return llm, model_name
        except Exception as e:
            last_error = e

    raise RuntimeError(f"No fue posible inicializar Gemini. Último error: {last_error}")


# =========================================================
# UTILIDADES
# =========================================================
def strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = strip_accents(text.lower().strip())
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_folder_name(text: str) -> str:
    text = normalize_text(text)
    text = text.replace("_", " ").replace("-", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_code(text: str) -> str:
    text = text.upper().strip()
    text = text.replace(" ", "-")
    text = re.sub(r"-+", "-", text)
    return text


def normalizar_respuesta_llm(content: Any) -> str:
    if content is None:
        return ""

    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        partes = []
        for item in content:
            if item is None:
                continue
            if isinstance(item, str):
                if item.strip():
                    partes.append(item.strip())
                continue
            if isinstance(item, dict):
                if "text" in item and item["text"]:
                    partes.append(str(item["text"]).strip())
                else:
                    partes.append(str(item).strip())
                continue
            texto = getattr(item, "text", None)
            if texto:
                partes.append(str(texto).strip())
            else:
                partes.append(str(item).strip())
        return "\n".join([p for p in partes if p]).strip()

    texto = getattr(content, "text", None)
    if texto:
        return str(texto).strip()

    return str(content).strip()


def clean_text(text: str, max_chars: int = MAX_TEXT_CHARS_POR_ARCHIVO) -> str:
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


def escape_drive_query_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def safe_invoke(llm, prompt_or_messages, retries: int = 4, base_wait: float = 2.0):
    last_error = None
    for intento in range(retries):
        try:
            return llm.invoke(prompt_or_messages)
        except Exception as e:
            last_error = e
            time.sleep(base_wait * (2 ** intento))
    raise last_error


def approx_size(item: Dict) -> int:
    contenido = item.get("contenido", "")
    if item.get("tipo") == "imagen":
        return 1800
    return len(contenido)


def chunk_items_dinamicamente(
    items: List[Dict],
    max_chars_por_tanda: int = MAX_TEXT_CHARS_POR_TANDA,
    max_archivos_por_tanda: int = MAX_ARCHIVOS_POR_TANDA,
) -> List[List[Dict]]:
    tandas = []
    actual = []
    chars_actuales = 0

    for item in items:
        size = approx_size(item)

        if size >= max_chars_por_tanda:
            if actual:
                tandas.append(actual)
                actual = []
                chars_actuales = 0
            tandas.append([item])
            continue

        if len(actual) >= max_archivos_por_tanda or (chars_actuales + size > max_chars_por_tanda):
            if actual:
                tandas.append(actual)
            actual = [item]
            chars_actuales = size
        else:
            actual.append(item)
            chars_actuales += size

    if actual:
        tandas.append(actual)

    return tandas


def dedupe_files(items: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for item in items:
        file_id = item.get("id")
        if file_id and file_id not in seen:
            out.append(item)
            seen.add(file_id)
    return out


# =========================================================
# DETECCION DE CONSULTA
# =========================================================
def parse_date_text(user_input: str) -> Optional[str]:
    text = normalize_text(user_input)

    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", text)
    if m:
        dia = int(m.group(1))
        mes = int(m.group(2))
        anio = int(m.group(3))
        try:
            return datetime(anio, mes, dia).strftime("%Y-%m-%d")
        except ValueError:
            return None

    m = re.search(r"\b(\d{1,2})-(\d{1,2})-(\d{4})\b", text)
    if m:
        dia = int(m.group(1))
        mes = int(m.group(2))
        anio = int(m.group(3))
        try:
            return datetime(anio, mes, dia).strftime("%Y-%m-%d")
        except ValueError:
            return None

    m = re.search(r"\b(\d{1,2})\s+de\s+([a-z]+)\s+de\s+(\d{4})\b", text)
    if m:
        dia = int(m.group(1))
        mes_txt = m.group(2)
        anio = int(m.group(3))
        mes = MESES.get(mes_txt)
        if mes:
            try:
                return datetime(anio, mes, dia).strftime("%Y-%m-%d")
            except ValueError:
                return None

    return None


def detect_well_codes(text: str) -> List[str]:
    found = set()
    upper_text = text.upper()

    patterns = [
        r"\b([A-Z]{2,6}-\d{1,3}[A-Z]?)\b",
        r"\b([A-Z]{2,6}\s\d{1,3}[A-Z]?)\b",
        r"\b([A-Z]{2,6}\d{1,3}[A-Z]?)\b",
    ]

    for pattern in patterns:
        for m in re.findall(pattern, upper_text):
            code = normalize_code(m)
            found.add(code)

    return sorted(found)


def build_code_variants(code: str) -> List[str]:
    code = normalize_code(code)
    return list({
        code,
        code.replace("-", " "),
        code.replace("-", ""),
    })


def extract_keywords(user_input: str, max_keywords: int = 5) -> List[str]:
    words = re.findall(r"[\w-]+", normalize_text(user_input))
    words = [w for w in words if len(w) > 2 and w not in STOPWORDS]
    return sorted(set(words), key=len, reverse=True)[:max_keywords]


def extract_quoted_name(user_input: str) -> Optional[str]:
    m = re.search(r'"([^"]+)"', user_input)
    if m:
        return m.group(1).strip()

    m = re.search(r"'([^']+)'", user_input)
    if m:
        return m.group(1).strip()

    return None

def extract_named_target(user_input: str) -> Optional[str]:
    """
    Detecta referencias a archivos/carpetas aunque no vengan entre comillas.
    Ejemplos:
    - archivo REPORTABILIDAD 2026
    - planilla REPORTABILIDAD 2026
    - carpeta Abril 2026
    """
    text = user_input.strip()

    patterns = [
        r'archivo\s+([A-ZÁÉÍÓÚ0-9 _\-\.]+)',
        r'planilla\s+([A-ZÁÉÍÓÚ0-9 _\-\.]+)',
        r'carpeta\s+([A-ZÁÉÍÓÚ0-9 _\-\.]+)',
        r'documento\s+([A-ZÁÉÍÓÚ0-9 _\-\.]+)',
        r'reporte\s+([A-ZÁÉÍÓÚ0-9 _\-\.]+)',
    ]

    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            candidate = m.group(1).strip(" .,:;")
            # corta si empieza otra frase
            candidate = re.split(
                r"\b(del|de la|de las|de los|con fecha|del dia|del día|para|que|donde|consultando)\b",
                candidate,
                flags=re.IGNORECASE
            )[0].strip(" .,:;")
            if len(candidate) >= 3:
                return candidate

    return None


def build_date_variants(fecha_iso: str) -> List[str]:
    """
    Genera variantes para matching contra nombres de hojas o carpetas.
    """
    if not fecha_iso:
        return []

    dt = datetime.strptime(fecha_iso, "%Y-%m-%d")
    dia = dt.day
    mes = dt.month
    anio = dt.year
    mes_nombre = MESES_NUM_A_NOMBRE[mes]

    variants = {
        fecha_iso,                               # 2026-04-23
        f"{dia:02d}-{mes:02d}-{anio}",          # 23-04-2026
        f"{dia}/{mes}/{anio}",                  # 23/4/2026
        f"{dia:02d}/{mes:02d}/{anio}",          # 23/04/2026
        f"{dia}-{mes}-{anio}",                  # 23-4-2026
        f"{dia} de {mes_nombre} de {anio}",     # 23 de abril de 2026
        f"{dia} {mes_nombre} {anio}",           # 23 abril 2026
        f"{dia:02d}",                           # 23
        str(dia),                               # 23
    }

    return list(variants)


def is_supported_analysis_mime(mime_type: str) -> bool:
    if mime_type == GOOGLE_FOLDER_MIME:
        return False

    if mime_type == GOOGLE_SHEET_MIME:
        return True

    if mime_type.startswith(IMAGE_MIME_PREFIX):
        return True

    if mime_type in {
        "application/pdf",
        "application/vnd.google-apps.document",
        "text/plain",
        "text/csv",
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }:
        return True

    if "csv" in mime_type or "excel" in mime_type or "spreadsheet" in mime_type:
        return True

    return False


def extract_google_drive_id_from_text(text: str) -> Optional[str]:
    patterns = [
        r"/d/([a-zA-Z0-9_-]{20,})",
        r"[?&]id=([a-zA-Z0-9_-]{20,})",
        r"/folders/([a-zA-Z0-9_-]{20,})",
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            return m.group(1)
    return None

def classify_query(user_input: str) -> Dict[str, bool]:
    text = normalize_text(user_input)

    serial_terms = [
        "serial", "serie", "numero de serie", "sn", "s/n",
        "placa", "etiqueta", "sticker", "seriales"
    ]
    activity_terms = [
        "que se hizo", "que labores", "labores", "actividades", "trabajos",
        "faenas", "tareas", "hitos", "intervenciones", "resumen"
    ]
    latest_terms = [
        "ultima vez", "ultimo registro", "ultimo", "mas reciente",
        "ultimo reporte", "registro mas reciente"
    ]
    install_terms = [
        "instalado", "instalados", "instalada", "instaladas",
        "equipos", "sensores", "instrumentos"
    ]
    access_terms = [
        "tienes acceso", "puedes ver", "puedes leer", "tienes acceso al archivo",
        "puedes acceder", "existe el archivo", "encuentra el archivo",
        "encuentras el archivo", "si tienes acceso", "si puedes ver"
    ]
    failure_terms = [
        "falla", "fallas", "fallo", "fallos", "averia", "averías", "alarma", "alarmas"
    ]

    fecha_iso = parse_date_text(user_input)
    quoted_name = extract_quoted_name(user_input)
    named_target = extract_named_target(user_input)

    return {
        "seriales": any(t in text for t in serial_terms),
        "actividades": any(t in text for t in activity_terms),
        "ultimos_registros": any(t in text for t in latest_terms),
        "instalacion": any(t in text for t in install_terms),
        "fecha_especifica": fecha_iso is not None,
        "consulta_diaria": fecha_iso is not None and any(t in text for t in activity_terms),
        "access_check": any(t in text for t in access_terms),
        "has_drive_url_or_id": extract_google_drive_id_from_text(user_input) is not None,
        "fallas_query": any(t in text for t in failure_terms),
        "has_explicit_target_name": quoted_name is not None or named_target is not None,
    }



# =========================================================
# GOOGLE DRIVE HELPERS
# =========================================================
@st.cache_data(show_spinner=False, ttl=600)
def find_folder_by_name(_service, folder_name: str) -> Optional[Dict]:
    q = (
        f"name = '{escape_drive_query_value(folder_name)}' and "
        f"mimeType = '{GOOGLE_FOLDER_MIME}' and trashed = false"
    )
    resp = (
        _service.files()
        .list(
            q=q,
            fields="files(id, name, mimeType, modifiedTime, parents, webViewLink, driveId)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    files = resp.get("files", [])
    return files[0] if files else None


@st.cache_data(show_spinner=False, ttl=600)
def list_children(_service, parent_id: str) -> List[Dict]:
    q = f"'{parent_id}' in parents and trashed = false"
    results = []
    page_token = None

    while True:
        resp = (
            _service.files()
            .list(
                q=q,
                fields="nextPageToken, files(id, name, mimeType, modifiedTime, parents, webViewLink, driveId)",
                pageSize=100,
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                orderBy="folder,name",
            )
            .execute()
        )
        results.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return results


def get_file_metadata(service, file_id: str) -> Optional[Dict]:
    try:
        return (
            service.files()
            .get(
                fileId=file_id,
                fields="id, name, mimeType, modifiedTime, parents, webViewLink, driveId",
                supportsAllDrives=True,
            )
            .execute()
        )
    except Exception:
        return None


def search_exact_name_global(service, exact_name: str, only_folders: bool = False) -> List[Dict]:
    mime_filter = f" and mimeType = '{GOOGLE_FOLDER_MIME}'" if only_folders else ""
    q = (
        f"name = '{escape_drive_query_value(exact_name)}' "
        f"and trashed = false"
        f"{mime_filter}"
    )

    try:
        resp = (
            service.files()
            .list(
                q=q,
                fields="files(id, name, mimeType, modifiedTime, parents, webViewLink, driveId)",
                pageSize=50,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                orderBy="modifiedTime desc",
            )
            .execute()
        )
        return resp.get("files", [])
    except Exception:
        return []


def search_name_contains_global(service, name_fragment: str, only_folders: bool = False) -> List[Dict]:
    mime_filter = f" and mimeType = '{GOOGLE_FOLDER_MIME}'" if only_folders else ""
    q = (
        f"name contains '{escape_drive_query_value(name_fragment)}' "
        f"and trashed = false"
        f"{mime_filter}"
    )

    try:
        resp = (
            service.files()
            .list(
                q=q,
                fields="files(id, name, mimeType, modifiedTime, parents, webViewLink, driveId)",
                pageSize=50,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                orderBy="modifiedTime desc",
            )
            .execute()
        )
        return resp.get("files", [])
    except Exception:
        return []


def pick_month_folder(children: List[Dict], month_num: int, year: int) -> Optional[Dict]:
    month_name = MESES_NUM_A_NOMBRE[month_num]

    exact_targets = {
        f"{month_name} {year}",
        f"{month_name}-{year}",
        f"{month_name}_{year}",
    }

    normalized_targets = {normalize_folder_name(t) for t in exact_targets}

    for item in children:
        if item["mimeType"] != GOOGLE_FOLDER_MIME:
            continue
        if normalize_folder_name(item["name"]) in normalized_targets:
            return item

    for item in children:
        if item["mimeType"] != GOOGLE_FOLDER_MIME:
            continue
        name_norm = normalize_folder_name(item["name"])
        if month_name in name_norm and str(year) in name_norm:
            return item

    return None


def pick_day_folder(children: List[Dict], day: int) -> Optional[Dict]:
    target_variants = {
        str(day),
        f"{day:02d}",
    }

    for item in children:
        if item["mimeType"] != GOOGLE_FOLDER_MIME:
            continue
        name_norm = normalize_folder_name(item["name"])
        if name_norm in target_variants:
            return item

    return None


def pick_well_folders(children: List[Dict], well_codes: List[str]) -> List[Dict]:
    if not well_codes:
        return []

    code_variants_norm = set()
    for code in well_codes:
        for v in build_code_variants(code):
            code_variants_norm.add(normalize_folder_name(v))

    matches = []
    for item in children:
        if item["mimeType"] != GOOGLE_FOLDER_MIME:
            continue
        name_norm = normalize_folder_name(item["name"])
        if name_norm in code_variants_norm:
            matches.append(item)

    return matches


def recursive_collect_files(service, folder_id: str, max_items: int = MAX_RECURSION_ITEMS) -> List[Dict]:
    collected = []
    queue = [folder_id]
    seen_folders = set()

    while queue and len(collected) < max_items:
        current = queue.pop(0)
        if current in seen_folders:
            continue
        seen_folders.add(current)

        children = list_children(service, current)
        for child in children:
            if child["mimeType"] == GOOGLE_FOLDER_MIME:
                queue.append(child["id"])
            else:
                collected.append(child)
                if len(collected) >= max_items:
                    break

    return collected


def recursive_collect_folder_and_files(service, folder_id: str, max_items: int = MAX_RECURSION_ITEMS) -> Tuple[List[Dict], List[Dict]]:
    folders = []
    files = []
    queue = [folder_id]
    seen = set()

    while queue and len(files) < max_items:
        current = queue.pop(0)
        if current in seen:
            continue
        seen.add(current)

        children = list_children(service, current)
        for child in children:
            if child["mimeType"] == GOOGLE_FOLDER_MIME:
                folders.append(child)
                queue.append(child["id"])
            else:
                files.append(child)
                if len(files) >= max_items:
                    break

    return folders, files


def search_folders_by_well_code_global(service, well_code: str) -> List[Dict]:
    candidates = []
    for variant in build_code_variants(well_code):
        candidates.extend(search_exact_name_global(service, variant, only_folders=True))
        candidates.extend(search_name_contains_global(service, variant, only_folders=True))
    candidates = dedupe_files(candidates)
    candidates.sort(key=lambda x: x.get("modifiedTime", ""), reverse=True)
    return candidates[:MAX_WELL_FOLDER_CANDIDATES]


# =========================================================
# BUSQUEDAS
# =========================================================
def search_by_url_or_id(service, user_input: str) -> Tuple[List[Dict], Optional[Dict]]:
    file_id = extract_google_drive_id_from_text(user_input)
    if not file_id:
        return [], None

    meta = get_file_metadata(service, file_id)
    if not meta:
        return [], None

    if meta["mimeType"] == GOOGLE_FOLDER_MIME:
        _, files = recursive_collect_folder_and_files(service, meta["id"])
        return files[:MAX_ARCHIVOS_EN_POOL], meta

    return [meta], meta

def search_access_target(service, user_input: str) -> Tuple[List[Dict], Optional[Dict]]:
    target_name = extract_quoted_name(user_input) or extract_named_target(user_input)
    if not target_name:
        return [], None

    exacts = search_exact_name_global(service, target_name, only_folders=False)
    if exacts:
        best = exacts[0]
        if best["mimeType"] == GOOGLE_FOLDER_MIME:
            _, files = recursive_collect_folder_and_files(service, best["id"])
            return files[:MAX_ARCHIVOS_EN_POOL], best
        return [best], best

    exact_folders = search_exact_name_global(service, target_name, only_folders=True)
    if exact_folders:
        best = exact_folders[0]
        _, files = recursive_collect_folder_and_files(service, best["id"])
        return files[:MAX_ARCHIVOS_EN_POOL], best

    contains_candidates = search_name_contains_global(service, target_name, only_folders=False)
    contains_candidates = dedupe_files(contains_candidates)
    if contains_candidates:
        best = contains_candidates[0]
        if best["mimeType"] == GOOGLE_FOLDER_MIME:
            _, files = recursive_collect_folder_and_files(service, best["id"])
            return files[:MAX_ARCHIVOS_EN_POOL], best
        return [best], best

    return [], None


def search_drive_general(service, user_input: str) -> List[Dict]:
    well_codes = detect_well_codes(user_input)
    keywords = extract_keywords(user_input)
    terms = []

    for code in well_codes:
        terms.extend(build_code_variants(code))
    terms.extend(keywords)

    quoted = extract_quoted_name(user_input)
    if quoted:
        terms.insert(0, quoted)

    if not terms:
        terms = [user_input.strip()]

    unique_terms = []
    seen = set()
    for t in terms:
        k = normalize_text(t)
        if k and k not in seen:
            unique_terms.append(t)
            seen.add(k)

    pool = []
    seen_ids = set()

    for term in unique_terms[:10]:
        q = (
            f"(name contains '{escape_drive_query_value(term)}' or "
            f"fullText contains '{escape_drive_query_value(term)}') and "
            f"trashed = false"
        )

        try:
            resp = (
                service.files()
                .list(
                    q=q,
                    fields="files(id, name, mimeType, modifiedTime, parents, webViewLink, driveId)",
                    orderBy="modifiedTime desc",
                    pageSize=TOP_RESULTADOS_POR_QUERY,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )
            for f in resp.get("files", []):
                if f["id"] not in seen_ids:
                    pool.append(f)
                    seen_ids.add(f["id"])
        except Exception:
            continue

    def score_file(f: Dict) -> Tuple[int, str]:
        score = 0
        name = normalize_text(f.get("name", ""))
        user_norm = normalize_text(user_input)

        for code in well_codes:
            for variant in build_code_variants(code):
                if normalize_text(variant) in name:
                    score += 10

        if quoted and normalize_text(quoted) == name:
            score += 50
        elif quoted and normalize_text(quoted) in name:
            score += 20

        if any(k in user_norm for k in ["serial", "serie", "placa", "etiqueta", "sensor"]):
            if any(x in name for x in ["sensor", "img", "foto", "image"]):
                score += 3

        return score, f.get("modifiedTime", "")

    pool.sort(key=lambda x: score_file(x), reverse=True)
    return pool[:MAX_ARCHIVOS_EN_POOL]


def search_drive_by_date_structure(service, user_input: str) -> List[Dict]:
    fecha_iso = parse_date_text(user_input)
    if not fecha_iso:
        return []

    dt = datetime.strptime(fecha_iso, "%Y-%m-%d")
    day = dt.day
    month = dt.month
    year = dt.year
    well_codes = detect_well_codes(user_input)

    for root_name in ROOT_DRIVE_FOLDER_NAMES:
        root = find_folder_by_name(service, root_name)
        if not root:
            continue

        root_children = list_children(service, root["id"])
        month_folder = pick_month_folder(root_children, month, year)
        if not month_folder:
            continue

        month_children = list_children(service, month_folder["id"])
        day_folder = pick_day_folder(month_children, day)
        if not day_folder:
            continue

        day_children = list_children(service, day_folder["id"])

        archivos = []
        seen_ids = set()

        for item in day_children:
            if item["mimeType"] != GOOGLE_FOLDER_MIME and item["id"] not in seen_ids:
                archivos.append(item)
                seen_ids.add(item["id"])

        matching_well_folders = pick_well_folders(day_children, well_codes)

        if matching_well_folders:
            for folder in matching_well_folders:
                encontrados = recursive_collect_files(service, folder["id"])
                for f in encontrados:
                    if f["id"] not in seen_ids:
                        archivos.append(f)
                        seen_ids.add(f["id"])
        else:
            for item in day_children:
                if item["mimeType"] == GOOGLE_FOLDER_MIME:
                    encontrados = recursive_collect_files(service, item["id"])
                    for f in encontrados:
                        if f["id"] not in seen_ids:
                            archivos.append(f)
                            seen_ids.add(f["id"])

        archivos.sort(key=lambda x: x.get("modifiedTime", ""))
        if archivos:
            return archivos[:MAX_ARCHIVOS_EN_POOL]

    return []


def search_drive_by_well_folder_global(service, user_input: str) -> List[Dict]:
    well_codes = detect_well_codes(user_input)
    if not well_codes:
        return []

    pool = []
    seen = set()

    for code in well_codes:
        folders = search_folders_by_well_code_global(service, code)
        for folder in folders:
            files = recursive_collect_files(service, folder["id"], max_items=MAX_RECURSION_ITEMS)
            for f in files:
                if f["id"] not in seen:
                    pool.append(f)
                    seen.add(f["id"])

    pool.sort(key=lambda x: x.get("modifiedTime", ""), reverse=True)
    return pool[:MAX_ARCHIVOS_EN_POOL]


def buscar_archivos_drive(service, user_input: str) -> Tuple[List[Dict], Dict]:
    flags = classify_query(user_input)
    metadata = {
        "search_mode": "general",
        "matched_target": None,
        "target_only": False,
    }

    if flags["has_drive_url_or_id"]:
        archivos, matched = search_by_url_or_id(service, user_input)
        if matched:
            metadata["search_mode"] = "url_or_id"
            metadata["matched_target"] = matched
            metadata["target_only"] = True
            return dedupe_files([f for f in archivos if is_supported_analysis_mime(f["mimeType"])]), metadata

    # Si el usuario nombró explícitamente un archivo/carpeta, prioriza eso aunque no esté preguntando por acceso
    if flags["has_explicit_target_name"] or flags["access_check"]:
        archivos, matched = search_access_target(service, user_input)
        if matched:
            metadata["search_mode"] = "exact_target"
            metadata["matched_target"] = matched
            metadata["target_only"] = True

            if matched["mimeType"] == GOOGLE_FOLDER_MIME:
                return dedupe_files([f for f in archivos if is_supported_analysis_mime(f["mimeType"])]), metadata

            # si es archivo individual, analiza solo ese
            return [matched], metadata

    if flags["fecha_especifica"]:
        por_estructura = search_drive_by_date_structure(service, user_input)
        if por_estructura:
            metadata["search_mode"] = "date_structure"
            return dedupe_files([f for f in por_estructura if is_supported_analysis_mime(f["mimeType"])]), metadata

    well_global = search_drive_by_well_folder_global(service, user_input)
    if well_global:
        metadata["search_mode"] = "well_global"
        return dedupe_files([f for f in well_global if is_supported_analysis_mime(f["mimeType"])]), metadata

    general = search_drive_general(service, user_input)
    metadata["search_mode"] = "general"
    return dedupe_files([f for f in general if is_supported_analysis_mime(f["mimeType"])]), metadata


# =========================================================
# DESCARGA / EXPORTACION DE ARCHIVOS
# =========================================================
def descargar_archivo_binario(service, file_id: str) -> BytesIO:
    request = service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh


def exportar_google_workspace(service, file_id: str, mime_type: str) -> Optional[BytesIO]:
    export_map = {
        "application/vnd.google-apps.document": "text/plain",
    }

    export_mime = export_map.get(mime_type)
    if not export_mime:
        return None

    request = service.files().export_media(fileId=file_id, mimeType=export_mime)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh


def get_file_bytes(service, file_id: str, mime_type: str) -> Optional[BytesIO]:
    if mime_type == GOOGLE_SHEET_MIME:
        return None
    if mime_type.startswith("application/vnd.google-apps."):
        return exportar_google_workspace(service, file_id, mime_type)
    return descargar_archivo_binario(service, file_id)


# =========================================================
# LECTURA DE GOOGLE SHEETS NATIVOS
# =========================================================
def quote_sheet_title_for_range(title: str) -> str:
    safe = title.replace("'", "''")
    return f"'{safe}'!A1:AN{MAX_SHEET_ROWS}"


def rows_to_text_table(rows: List[List[Any]], max_cols: int = MAX_SHEET_COLS) -> str:
    if not rows:
        return ""

    clipped_rows = []
    for row in rows[:MAX_SHEET_ROWS]:
        clipped = [str(cell) for cell in row[:max_cols]]
        clipped_rows.append(clipped)

    max_len = max(len(r) for r in clipped_rows)
    normalized = []
    for row in clipped_rows:
        normalized.append(row + [""] * (max_len - len(row)))

    if len(normalized) == 1:
        return " | ".join(normalized[0]).strip()

    header = normalized[0]
    data = normalized[1:]

    try:
        # evita columnas duplicadas o vacías
        header_limpio = []
        usados = {}
        for i, col in enumerate(header):
            col = str(col).strip() if str(col).strip() else f"col_{i+1}"
            if col in usados:
                usados[col] += 1
                col = f"{col}_{usados[col]}"
            else:
                usados[col] = 1
            header_limpio.append(col)

        df = pd.DataFrame(data, columns=header_limpio)
        df = df.fillna("")
        return df.astype(str).to_string(index=False)
    except Exception:
        lineas = []
        for row in normalized:
            lineas.append(" | ".join(row))
        return "\n".join(lineas)
        
def leer_google_sheet_nativo(
    sheets_service,
    file_id: str,
    file_name: str,
    fecha_mod: str,
    user_input: Optional[str] = None
) -> Optional[Dict]:
    try:
        meta = (
            sheets_service.spreadsheets()
            .get(spreadsheetId=file_id)
            .execute()
        )

        sheets = meta.get("sheets", [])
        if not sheets:
            debug_log(f"El archivo {file_name} no tiene hojas visibles.")
            return None

        fecha_iso = parse_date_text(user_input or "")
        date_variants_norm = {normalize_folder_name(v) for v in build_date_variants(fecha_iso)} if fecha_iso else set()

        # 1) priorizar hojas que coincidan con la fecha
        prioritized = []
        others = []

        for sh in sheets[:MAX_SHEETS_TABS]:
            props = sh.get("properties", {})
            title = props.get("title", "")
            if not title:
                continue

            title_norm = normalize_folder_name(title)

            if date_variants_norm and title_norm in date_variants_norm:
                prioritized.append(sh)
            elif date_variants_norm and any(v in title_norm for v in date_variants_norm if len(v) > 2):
                prioritized.append(sh)
            else:
                others.append(sh)

        # si hay match de fecha, lee primero esas hojas
        sheets_to_read = prioritized if prioritized else sheets[:MAX_SHEETS_TABS]

        bloques = []
        hojas_procesadas = 0

        for sh in sheets_to_read:
            props = sh.get("properties", {})
            title = props.get("title", "")
            if not title:
                continue

            try:
                rango = f"'{title}'!A1:AN{MAX_SHEET_ROWS}"
                values_resp = (
                    sheets_service.spreadsheets()
                    .values()
                    .get(
                        spreadsheetId=file_id,
                        range=rango,
                        majorDimension="ROWS"
                    )
                    .execute()
                )

                values = values_resp.get("values", [])
                if not values:
                    continue

                tabla = rows_to_text_table(values, max_cols=MAX_SHEET_COLS)
                tabla = clean_text(tabla, max_chars=7000)

                if tabla.strip():
                    bloques.append(f"HOJA: {title}\n{tabla}")
                    hojas_procesadas += 1

            except Exception as e_hoja:
                debug_log(f"Error leyendo hoja '{title}' de '{file_name}': {str(e_hoja)}")
                continue

        # si no encontró nada en hojas priorizadas, usa fallback con algunas otras
        if not bloques and prioritized:
            for sh in others[:5]:
                props = sh.get("properties", {})
                title = props.get("title", "")
                if not title:
                    continue

                try:
                    rango = f"'{title}'!A1:AN{MAX_SHEET_ROWS}"
                    values_resp = (
                        sheets_service.spreadsheets()
                        .values()
                        .get(
                            spreadsheetId=file_id,
                            range=rango,
                            majorDimension="ROWS"
                        )
                        .execute()
                    )

                    values = values_resp.get("values", [])
                    if not values:
                        continue

                    tabla = rows_to_text_table(values, max_cols=MAX_SHEET_COLS)
                    tabla = clean_text(tabla, max_chars=5000)

                    if tabla.strip():
                        bloques.append(f"HOJA: {title}\n{tabla}")
                        hojas_procesadas += 1

                except Exception as e_hoja:
                    debug_log(f"Error fallback leyendo hoja '{title}' de '{file_name}': {str(e_hoja)}")
                    continue

        if not bloques:
            debug_log(f"No se pudo extraer contenido útil de ninguna hoja en {file_name}.")
            return None

        fecha_legible = fecha_mod.split("T")[0] if fecha_mod else ""
        contenido = "\n\n".join(bloques)

        if prioritized:
            debug_log(
                f"Google Sheet leído correctamente: {file_name} | hojas priorizadas por fecha: {len(prioritized)} | hojas procesadas: {hojas_procesadas}"
            )
        else:
            debug_log(
                f"Google Sheet leído correctamente: {file_name} | hojas procesadas: {hojas_procesadas}"
            )

        return {
            "tipo": "texto",
            "contenido": clean_text(contenido, max_chars=MAX_TEXT_CHARS_POR_ARCHIVO),
            "nombre": file_name,
            "fecha": fecha_legible,
            "mime_type": GOOGLE_SHEET_MIME,
        }

    except Exception as e:
        debug_log(f"Error general leyendo Google Sheet '{file_name}': {str(e)}")
        return None




# =========================================================
# LECTURA DE ARCHIVOS
# =========================================================
def leer_pdf(fh: BytesIO) -> str:
    try:
        reader = PdfReader(fh)
        textos = []
        for page in reader.pages[:MAX_PAGINAS_PDF]:
            try:
                txt = page.extract_text() or ""
                if txt:
                    textos.append(txt)
            except Exception:
                continue
        return clean_text(" ".join(textos))
    except Exception:
        return ""


def leer_excel_o_csv(fh: BytesIO, mime_type: str) -> str:
    try:
        fh.seek(0)

        if "csv" in mime_type or mime_type == "text/plain":
            try:
                df = pd.read_csv(fh, nrows=MAX_FILAS_TABLA)
            except Exception:
                fh.seek(0)
                try:
                    df = pd.read_csv(fh, nrows=MAX_FILAS_TABLA, encoding="latin-1")
                except Exception:
                    fh.seek(0)
                    raw = fh.read().decode("utf-8", errors="ignore")
                    return clean_text(raw)
        else:
            df = pd.read_excel(fh, nrows=MAX_FILAS_TABLA)

        df = df.fillna("")
        txt = df.astype(str).head(MAX_FILAS_TABLA).to_string(index=False)
        return clean_text(txt)
    except Exception:
        return ""


def leer_texto_plano(fh: BytesIO) -> str:
    try:
        fh.seek(0)
        raw = fh.read().decode("utf-8", errors="ignore")
        return clean_text(raw)
    except Exception:
        return ""


def leer_imagen_base64(fh: BytesIO) -> Optional[str]:
    try:
        img = Image.open(fh).convert("RGB")
        img.thumbnail((1100, 1100))
        buffered = BytesIO()
        img.save(buffered, format="JPEG", quality=75)
        return base64.b64encode(buffered.getvalue()).decode("utf-8")
    except Exception:
        return None

def leer_archivo_multimodal(drive_service, sheets_service, file_id, mime_type, file_name, fecha_mod, user_input=None):
    try:
        fecha_legible = fecha_mod.split("T")[0] if fecha_mod else ""

        if mime_type == GOOGLE_SHEET_MIME:
            debug_log(f"Intentando leer Google Sheet nativo: {file_name}")
            return leer_google_sheet_nativo(
                sheets_service=sheets_service,
                file_id=file_id,
                file_name=file_name,
                fecha_mod=fecha_mod,
                user_input=user_input,
            )

        fh = get_file_bytes(drive_service, file_id, mime_type)
        if fh is None:
            if mime_type == GOOGLE_FOLDER_MIME:
                return None
            debug_log(f"No se pudieron obtener bytes del archivo: {file_name} | MIME: {mime_type}")
            return None

        if mime_type.startswith(IMAGE_MIME_PREFIX):
            encoded = leer_imagen_base64(fh)
            if not encoded:
                debug_log(f"No se pudo procesar la imagen: {file_name}")
                return None
            return {
                "tipo": "imagen",
                "contenido": f"data:image/jpeg;base64,{encoded}",
                "nombre": file_name,
                "fecha": fecha_legible,
                "mime_type": mime_type,
            }

        if mime_type == "application/pdf":
            texto = leer_pdf(fh)
            if not texto:
                debug_log(f"No se pudo extraer texto del PDF: {file_name}")
                return None
            return {
                "tipo": "texto",
                "contenido": texto,
                "nombre": file_name,
                "fecha": fecha_legible,
                "mime_type": mime_type,
            }

        if (
            "spreadsheet" in mime_type
            or "csv" in mime_type
            or "excel" in mime_type
            or mime_type == "text/plain"
        ):
            texto = leer_excel_o_csv(fh, mime_type)
            if not texto:
                debug_log(f"No se pudo extraer texto de planilla/CSV: {file_name}")
                return None
            return {
                "tipo": "texto",
                "contenido": texto,
                "nombre": file_name,
                "fecha": fecha_legible,
                "mime_type": mime_type,
            }

        if mime_type == "application/vnd.google-apps.document":
            texto = leer_texto_plano(fh)
            if not texto:
                debug_log(f"No se pudo extraer texto del Google Doc: {file_name}")
                return None
            return {
                "tipo": "texto",
                "contenido": texto,
                "nombre": file_name,
                "fecha": fecha_legible,
                "mime_type": mime_type,
            }

        debug_log(f"MIME no soportado o sin lector asignado: {file_name} | {mime_type}")
        return None

    except Exception as e:
        debug_log(f"Error general en leer_archivo_multimodal para {file_name}: {str(e)}")
        return None


# =========================================================
# RESPUESTAS DIRECTAS DE ACCESO
# =========================================================
def construir_respuesta_acceso_directo(matched_target: Dict, archivos_totales: List[Dict]) -> str:
    tipo = "carpeta" if matched_target.get("mimeType") == GOOGLE_FOLDER_MIME else "archivo"
    fecha = matched_target.get("modifiedTime", "")
    fecha_legible = fecha.split("T")[0] if fecha else "sin fecha"

    respuesta = [
        f"Sí, tengo acceso al {tipo} **{matched_target.get('name', '')}**.",
        f"Fecha de modificación: **{fecha_legible}**.",
    ]

    if matched_target.get("webViewLink"):
        respuesta.append(f"Enlace: {matched_target['webViewLink']}")

    if tipo == "carpeta":
        respuesta.append(f"Archivos detectados dentro de la carpeta: **{len(archivos_totales)}**.")
    else:
        respuesta.append(f"Tipo MIME: **{matched_target.get('mimeType', 'desconocido')}**.")

    return "\n\n".join(respuesta)


# =========================================================
# PROMPTS
# =========================================================
def construir_prompt_resumen_tanda(user_input: str, items_tanda: List[Dict], search_metadata: Dict) -> List[Dict]:
    query_flags = classify_query(user_input)
    fecha_texto = parse_date_text(user_input)
    pozos = detect_well_codes(user_input)

    matched_target = search_metadata.get("matched_target")
    search_mode = search_metadata.get("search_mode", "general")

    contexto_busqueda = ""
    if matched_target:
        contexto_busqueda = f"""
CONTEXTO DE BÚSQUEDA:
- Se encontró un objetivo específico accesible en Drive.
- Nombre objetivo: {matched_target.get('name', '')}
- Tipo objetivo: {matched_target.get('mimeType', '')}
- Modo de búsqueda: {search_mode}
""".strip()

    instrucciones_base = f"""
Analiza los archivos entregados y responde SOLO a partir de la evidencia disponible en documentos e imágenes.

CONSULTA DEL USUARIO:
{user_input}

CONTEXTO:
- Los archivos provienen de Google Drive y pueden incluir elementos compartidos por otras cuentas.
- Las imágenes, PDFs, planillas y documentos son evidencia válida.
- Debes usar tanto el texto extraído como la inspección visual de las imágenes.
{contexto_busqueda}

REGLAS OBLIGATORIAS:
1. No inventes información.
2. Usa únicamente evidencia encontrada en estos archivos.
3. Si algo no es legible o no se puede confirmar, indícalo explícitamente.
4. Siempre cita el nombre del archivo fuente y su fecha.
5. Si hay imágenes, inspecciona visualmente etiquetas, placas, instrumentos, sensores, tableros, pantallas, textos visibles, trabajos realizados y contexto de terreno.
6. Si la pregunta pide seriales, extrae SOLO seriales claramente visibles o explícitos en el texto.
7. Si la pregunta pide actividades de un día, trata cada archivo del día como evidencia de actividad realizada.
8. Si la pregunta pide fallas, identifica cantidad, tipo de falla, pozo o sensor afectado y evidencia específica.
9. Si hay conflicto entre archivos, menciónalo.
10. Si un archivo no aporta a la consulta, dilo brevemente.
11. Responde de forma estructurada, compacta y técnica.
""".strip()

    reglas_especificas = []

    if query_flags["seriales"]:
        reglas_especificas.append("""
CASO ESPECIAL: SERIALES / PLACAS / ETIQUETAS
- Busca números de serie, S/N, SN, códigos de equipo, modelos, etiquetas o placas.
- Si un serial aparece incompleto o borroso, márcalo como dudoso o parcial.
- No completes caracteres faltantes.
- Indica sensor o equipo asociado y nivel de confianza: alta, media o baja.
""".strip())

    if query_flags["actividades"] or query_flags["fecha_especifica"]:
        reglas_especificas.append(f"""
CASO ESPECIAL: ACTIVIDADES / BITÁCORA DIARIA
- Si la consulta es sobre qué se hizo, describe actividades, labores, mediciones, instalaciones, inspecciones, reparaciones, configuración, pruebas y hallazgos.
- Trata cada archivo como evidencia de un evento técnico.
- Organiza cronológicamente cuando sea posible.
- Si el usuario menciona una fecha ({fecha_texto if fecha_texto else "sin fecha explícita"}), prioriza evidencia de ese día.
- Si un archivo parece no corresponder realmente a ese día, indícalo como posible evidencia no concluyente.
""".strip())

    if query_flags["fallas_query"]:
        reglas_especificas.append(f"""
CASO ESPECIAL: FALLAS
- Identifica fallas de pozos y sensores.
- Cuenta cuántas fallas aparecen para la fecha consultada ({fecha_texto if fecha_texto else "sin fecha explícita"}).
- Lista cada falla por separado.
- Para cada una indica: pozo o sensor afectado, tipo de falla, evidencia y archivo fuente.
- Si una fila parece no corresponder a la fecha pedida, no la cuentes.
""".strip())

    if query_flags["ultimos_registros"]:
        reglas_especificas.append("""
CASO ESPECIAL: ÚLTIMO REGISTRO
- Determina cuál es la evidencia más reciente relacionada con la consulta.
- Indica claramente cuál parece ser el último registro encontrado y qué evidencia aporta.
""".strip())

    if query_flags["instalacion"]:
        reglas_especificas.append("""
CASO ESPECIAL: EQUIPOS / SENSORES INSTALADOS
- Identifica sensores, equipos, instrumentos o componentes instalados si se mencionan en texto o se observan en imágenes.
- Distingue entre equipo visible y equipo efectivamente confirmado como instalado.
""".strip())

    if pozos:
        reglas_especificas.append(f"""
POZOS O ACTIVOS PRIORIZADOS:
- Prioriza evidencia relacionada con: {", ".join(pozos)}
- Si aparece evidencia de otros pozos, solo inclúyela si ayuda a contextualizar o si hay ambigüedad.
""".strip())

    formato_respuesta = """
FORMATO DE RESPUESTA PARA ESTA TANDA:
- Archivo:
- Fecha:
- Relevancia para la consulta:
- Evidencia encontrada:
- Datos técnicos extraídos:
- Observaciones / dudas:
""".strip()

    instrucciones = "\n\n".join(
        [instrucciones_base] + reglas_especificas + [formato_respuesta]
    )

    msg_content = [{"type": "text", "text": instrucciones}]

    for item in items_tanda:
        bloque = (
            f"\n\n=== ARCHIVO ===\n"
            f"Nombre: {item['nombre']}\n"
            f"Fecha: {item['fecha']}\n"
            f"Tipo: {item.get('mime_type', item['tipo'])}\n"
        )

        if item["tipo"] == "texto":
            bloque += f"Contenido:\n{item['contenido']}\n"
            msg_content[0]["text"] += bloque
        else:
            bloque += "Contenido: imagen adjunta para inspección visual\n"
            msg_content[0]["text"] += bloque
            msg_content.append(
                {"type": "image_url", "image_url": {"url": item["contenido"]}}
            )

    return msg_content


def construir_prompt_final(user_input: str, resumenes_parciales: List[str], search_metadata: Dict) -> str:
    query_flags = classify_query(user_input)
    hallazgos = "\n\n".join(
        normalizar_respuesta_llm(r) for r in resumenes_parciales if r
    ).strip()

    matched_target = search_metadata.get("matched_target")
    search_mode = search_metadata.get("search_mode", "general")

    contexto_extra = ""
    if matched_target:
        contexto_extra = f"""
CONTEXTO DE ACCESO / UBICACIÓN:
- Objetivo encontrado: {matched_target.get('name', '')}
- Tipo MIME: {matched_target.get('mimeType', '')}
- Modo de búsqueda: {search_mode}
""".strip()

    instrucciones = [
        f"""
Genera una respuesta final basada EXCLUSIVAMENTE en los hallazgos parciales siguientes.

CONSULTA:
{user_input}

HALLAZGOS PARCIALES:
{hallazgos}

{contexto_extra}
""".strip(),
        """
REGLAS GENERALES:
1. No uses introducciones de cortesía.
2. No inventes información.
3. Responde solo con evidencia encontrada.
4. Incluye siempre nombre del archivo y fecha cuando cites evidencia.
5. Si algo es ambiguo o no confirmado, indícalo claramente.
6. Si no hay evidencia suficiente, dilo explícitamente.
7. Prioriza precisión por sobre redacción adornada.
8. Cuando corresponda, resume por fecha, pozo, actividad o equipo.
""".strip()
    ]

    if query_flags["seriales"]:
        instrucciones.append("""
FORMATO RECOMENDADO PARA SERIALES:
- Sensor / equipo
- Número de serie
- Archivo fuente
- Fecha
- Confianza
- Observación
""".strip())

    if query_flags["actividades"] or query_flags["fecha_especifica"]:
        instrucciones.append("""
FORMATO RECOMENDADO PARA ACTIVIDADES:
- Fecha
- Actividad / labor detectada
- Evidencia observada
- Archivo fuente
- Observación
""".strip())

    if query_flags["fallas_query"]:
        instrucciones.append("""
FORMATO RECOMENDADO PARA FALLAS:
- Total de fallas detectadas
- Lista de fallas
  - Pozo o sensor afectado
  - Tipo de falla
  - Fecha
  - Archivo fuente
  - Evidencia
  - Observación
""".strip())

    if query_flags["ultimos_registros"]:
        instrucciones.append("""
FORMATO RECOMENDADO PARA ÚLTIMO REGISTRO:
- Último registro identificado
- Fecha
- Archivo fuente
- Evidencia clave
- Observación o limitación
""".strip())

    if query_flags["access_check"]:
        instrucciones.append("""
Si la consulta era sobre acceso o existencia:
- Indica claramente si el archivo o carpeta fue encontrado.
- Si fue encontrado, resume qué tipo de contenido contiene.
- Si no fue encontrado, dilo explícitamente.
""".strip())

    if not any(query_flags.values()):
        instrucciones.append("""
Si la consulta es abierta, responde de forma técnica y estructurada:
- Respuesta principal
- Evidencias encontradas
- Archivos fuente
- Observaciones
""".strip())

    return "\n\n".join(instrucciones)


# =========================================================
# FLUJO PRINCIPAL
# =========================================================
user_input = st.chat_input("Escriba su consulta aquí...")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})

    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        try:
            drive_service, sheets_service = get_google_services()
            llm, model_name = get_working_llm()
            st.caption(f"Modelo en uso: {model_name}")

            with st.spinner("Buscando archivos relevantes en Drive..."):
                archivos_totales, search_metadata = buscar_archivos_drive(drive_service, user_input)

            matched_target = search_metadata.get("matched_target")
            query_flags = classify_query(user_input)

            if query_flags["access_check"] and matched_target:
                st.success("Objetivo encontrado en Drive.")
                st.markdown(construir_respuesta_acceso_directo(matched_target, archivos_totales))

            if not archivos_totales:
                if query_flags["access_check"]:
                    st.warning("No encontré un archivo o carpeta accesible que coincida con esa referencia.")
                else:
                    st.warning("No se encontraron registros relacionados en Drive.")
                st.stop()

            with st.expander("Ver archivos seleccionados", expanded=False):
                if matched_target:
                    st.write(
                        f"Objetivo encontrado: {matched_target.get('name', '')} | "
                        f"{matched_target.get('mimeType', '')} | "
                        f"{matched_target.get('modifiedTime', '')}"
                    )
                    st.write(f"Modo de búsqueda: {search_metadata.get('search_mode', '')}")
                    st.write("---")

                for f in archivos_totales[:50]:
                    st.write(
                        f"- {f['name']} | {f.get('modifiedTime', '')} | {f.get('mimeType', '')}"
                    )

            contenidos = []
            with st.spinner("Descargando y leyendo archivos..."):
                for f in archivos_totales:
                    res = leer_archivo_multimodal(
                    drive_service=drive_service,
                    sheets_service=sheets_service,
                    file_id=f["id"],
                    mime_type=f["mimeType"],
                    file_name=f["name"],
                    fecha_mod=f.get("modifiedTime", ""),
                    user_input=user_input,
                )
                    if res:
                        contenidos.append(res)

            if not contenidos:
                st.error("Se encontraron archivos, pero no se pudo extraer contenido útil.")
                st.info(
                    "Revisa el bloque de 'Depuración'. Si aparece un error relacionado con Google Sheets API, "
                    "normalmente significa que la API de Sheets no está habilitada o que la lectura de hojas falló."
                )
                st.stop()

            tandas = chunk_items_dinamicamente(contenidos)

            if not tandas:
                st.warning("No se pudieron formar tandas válidas para análisis.")
                st.stop()

            st.info(
                f"Se analizarán {len(contenidos)} archivos útiles en {len(tandas)} tanda(s)."
            )

            progress_bar = st.progress(0.0)
            resumenes_parciales = []

            for i, tanda in enumerate(tandas, start=1):
                with st.spinner(f"Analizando tanda {i}/{len(tandas)}..."):
                    msg_content = construir_prompt_resumen_tanda(user_input, tanda, search_metadata)

                    try:
                        resp_tanda = safe_invoke(
                            llm,
                            [HumanMessage(content=msg_content)],
                            retries=4
                        )

                        contenido_tanda = normalizar_respuesta_llm(
                            getattr(resp_tanda, "content", "")
                        )

                        if contenido_tanda:
                            resumenes_parciales.append(contenido_tanda)
                        else:
                            resumenes_parciales.append(
                                f"[Tanda {i} procesada, pero sin contenido textual útil]"
                            )

                    except Exception as e:
                        resumenes_parciales.append(
                            f"[Tanda {i} no procesada por error: {str(e)}]"
                        )

                progress_bar.progress(i / len(tandas))

            if not resumenes_parciales:
                st.error("No fue posible generar resúmenes parciales.")
                st.stop()

            with st.spinner("Consolidando respuesta final..."):
                prompt_final = construir_prompt_final(user_input, resumenes_parciales, search_metadata)

                respuesta_final = safe_invoke(
                    llm,
                    [HumanMessage(content=prompt_final)],
                    retries=4
                )

            contenido_final = normalizar_respuesta_llm(
                getattr(respuesta_final, "content", "")
            )

            if not contenido_final:
                contenido_final = "No fue posible generar una respuesta final con contenido útil."

            st.markdown(contenido_final)
            st.session_state.messages.append(
                {"role": "assistant", "content": contenido_final}
            )

        except Exception as e:
            st.error(f"Error técnico: {str(e)}")
