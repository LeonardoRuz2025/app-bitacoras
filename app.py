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

ROOT_DRIVE_FOLDER_NAME = "Bitacoras 2025"

MAX_ARCHIVOS_EN_POOL = 120
MAX_TEXT_CHARS_POR_ARCHIVO = 12000
MAX_TEXT_CHARS_POR_TANDA = 26000
MAX_ARCHIVOS_POR_TANDA = 6
MAX_PAGINAS_PDF = 8
MAX_FILAS_TABLA = 60
MAX_RECURSION_ITEMS = 300
TOP_RESULTADOS_POR_QUERY = 25

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
def get_drive_service():
    info_claves = json.loads(st.secrets["GOOGLE_JSON_COMPLETO"])
    creds = service_account.Credentials.from_service_account_info(info_claves)
    return build("drive", "v3", credentials=creds)


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


def extract_keywords(user_input: str, max_keywords: int = 4) -> List[str]:
    words = re.findall(r"[\w-]+", normalize_text(user_input))
    words = [w for w in words if len(w) > 2 and w not in STOPWORDS]
    return sorted(set(words), key=len, reverse=True)[:max_keywords]


def classify_query(user_input: str) -> Dict[str, bool]:
    text = normalize_text(user_input)

    serial_terms = [
        "serial", "serie", "numero de serie", "sn", "s/n",
        "placa", "etiqueta", "sticker", "seriales"
    ]
    activity_terms = [
        "que se hizo", "que labores", "labores", "actividades", "trabajos",
        "faenas", "tareas", "hitos", "intervenciones"
    ]
    latest_terms = [
        "ultima vez", "ultimo registro", "ultimo", "mas reciente",
        "ultimo reporte", "registro mas reciente"
    ]
    install_terms = [
        "instalado", "instalados", "instalada", "instaladas",
        "equipos", "sensores", "instrumentos"
    ]

    fecha_iso = parse_date_text(user_input)

    return {
        "seriales": any(t in text for t in serial_terms),
        "actividades": any(t in text for t in activity_terms),
        "ultimos_registros": any(t in text for t in latest_terms),
        "instalacion": any(t in text for t in install_terms),
        "fecha_especifica": fecha_iso is not None,
        "consulta_diaria": fecha_iso is not None and any(t in text for t in activity_terms),
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
            fields="files(id, name, mimeType, modifiedTime, parents)",
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
                fields="nextPageToken, files(id, name, mimeType, modifiedTime, parents)",
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

    # fallback: contiene mes y año
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


def search_drive_general(service, user_input: str) -> List[Dict]:
    well_codes = detect_well_codes(user_input)
    keywords = extract_keywords(user_input)
    terms = []

    for code in well_codes:
        terms.extend(build_code_variants(code))
    terms.extend(keywords)

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

    for term in unique_terms[:8]:
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
                    fields="files(id, name, mimeType, modifiedTime, parents)",
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

    root = find_folder_by_name(service, ROOT_DRIVE_FOLDER_NAME)
    if not root:
        return []

    root_children = list_children(service, root["id"])
    month_folder = pick_month_folder(root_children, month, year)
    if not month_folder:
        return []

    month_children = list_children(service, month_folder["id"])
    day_folder = pick_day_folder(month_children, day)
    if not day_folder:
        return []

    day_children = list_children(service, day_folder["id"])

    archivos = []
    seen_ids = set()

    # Archivos sueltos del día
    for item in day_children:
        if item["mimeType"] != GOOGLE_FOLDER_MIME and item["id"] not in seen_ids:
            archivos.append(item)
            seen_ids.add(item["id"])

    # Si el usuario menciona pozo, priorizar esa subcarpeta
    matching_well_folders = pick_well_folders(day_children, well_codes)

    if matching_well_folders:
        for folder in matching_well_folders:
            encontrados = recursive_collect_files(service, folder["id"])
            for f in encontrados:
                if f["id"] not in seen_ids:
                    archivos.append(f)
                    seen_ids.add(f["id"])
    else:
        # Si no menciona pozo, recorrer todas las subcarpetas del día
        for item in day_children:
            if item["mimeType"] == GOOGLE_FOLDER_MIME:
                encontrados = recursive_collect_files(service, item["id"])
                for f in encontrados:
                    if f["id"] not in seen_ids:
                        archivos.append(f)
                        seen_ids.add(f["id"])

    # Orden cronológico ascendente dentro del día
    archivos.sort(key=lambda x: x.get("modifiedTime", ""))
    return archivos[:MAX_ARCHIVOS_EN_POOL]


def buscar_archivos_drive(service, user_input: str) -> List[Dict]:
    flags = classify_query(user_input)

    # Si hay fecha explícita, primero intenta navegación estructural
    if flags["fecha_especifica"]:
        por_estructura = search_drive_by_date_structure(service, user_input)
        if por_estructura:
            return por_estructura

    # Fallback general
    return search_drive_general(service, user_input)


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
        "application/vnd.google-apps.spreadsheet": "text/csv",
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
    if mime_type.startswith("application/vnd.google-apps."):
        return exportar_google_workspace(service, file_id, mime_type)
    return descargar_archivo_binario(service, file_id)


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


def leer_archivo_multimodal(service, file_id, mime_type, file_name, fecha_mod):
    try:
        fh = get_file_bytes(service, file_id, mime_type)
        if fh is None:
            return None

        fecha_legible = fecha_mod.split("T")[0] if fecha_mod else ""

        if mime_type.startswith(IMAGE_MIME_PREFIX):
            encoded = leer_imagen_base64(fh)
            if not encoded:
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
            or mime_type == "application/vnd.google-apps.spreadsheet"
        ):
            texto = leer_excel_o_csv(fh, mime_type)
            if not texto:
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
                return None
            return {
                "tipo": "texto",
                "contenido": texto,
                "nombre": file_name,
                "fecha": fecha_legible,
                "mime_type": mime_type,
            }

        return None

    except Exception:
        return None


# =========================================================
# PROMPTS
# =========================================================
def construir_prompt_resumen_tanda(user_input: str, items_tanda: List[Dict]) -> List[Dict]:
    query_flags = classify_query(user_input)
    fecha_texto = parse_date_text(user_input)
    pozos = detect_well_codes(user_input)

    instrucciones_base = f"""
Analiza los archivos entregados y responde SOLO a partir de la evidencia disponible en documentos e imágenes.

CONSULTA DEL USUARIO:
{user_input}

CONTEXTO:
- Los archivos provienen de un Drive de bitácoras técnicas de terreno.
- Las imágenes, PDFs, planillas y documentos son evidencia válida.
- Debes usar tanto el texto extraído como la inspección visual de las imágenes.

REGLAS OBLIGATORIAS:
1. No inventes información.
2. Usa únicamente evidencia encontrada en estos archivos.
3. Si algo no es legible o no se puede confirmar, indícalo explícitamente.
4. Siempre cita el nombre del archivo fuente y su fecha.
5. Si hay imágenes, inspecciona visualmente etiquetas, placas, instrumentos, sensores, tableros, pantallas, textos visibles, trabajos realizados y contexto de terreno.
6. Si la pregunta pide seriales, extrae SOLO seriales claramente visibles o explícitos en el texto.
7. Si la pregunta pide actividades de un día, trata cada archivo del día como evidencia de actividad realizada.
8. Si hay conflicto entre archivos, menciónalo.
9. Si un archivo no aporta a la consulta, dilo brevemente.
10. Responde de forma estructurada, compacta y técnica.
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


def construir_prompt_final(user_input: str, resumenes_parciales: List[str]) -> str:
    query_flags = classify_query(user_input)
    hallazgos = "\n\n".join(
        normalizar_respuesta_llm(r) for r in resumenes_parciales if r
    ).strip()

    instrucciones = [
        f"""
Genera una respuesta final basada EXCLUSIVAMENTE en los hallazgos parciales siguientes.

CONSULTA:
{user_input}

HALLAZGOS PARCIALES:
{hallazgos}
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

    if query_flags["ultimos_registros"]:
        instrucciones.append("""
FORMATO RECOMENDADO PARA ÚLTIMO REGISTRO:
- Último registro identificado
- Fecha
- Archivo fuente
- Evidencia clave
- Observación o limitación
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
            service = get_drive_service()
            llm, model_name = get_working_llm()
            st.caption(f"Modelo en uso: {model_name}")

            with st.spinner("Buscando archivos relevantes en Drive..."):
                archivos_totales = buscar_archivos_drive(service, user_input)

            if not archivos_totales:
                st.warning("No se encontraron registros relacionados en Drive.")
                st.stop()

            with st.expander("Ver archivos seleccionados", expanded=False):
                for f in archivos_totales[:40]:
                    st.write(f"- {f['name']} | {f.get('modifiedTime', '')} | {f.get('mimeType', '')}")

            contenidos = []
            with st.spinner("Descargando y leyendo archivos..."):
                for f in archivos_totales:
                    res = leer_archivo_multimodal(
                        service=service,
                        file_id=f["id"],
                        mime_type=f["mimeType"],
                        file_name=f["name"],
                        fecha_mod=f.get("modifiedTime", ""),
                    )
                    if res:
                        contenidos.append(res)

            if not contenidos:
                st.warning("Se encontraron archivos, pero no se pudo extraer contenido útil.")
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
                    msg_content = construir_prompt_resumen_tanda(user_input, tanda)

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
                prompt_final = construir_prompt_final(user_input, resumenes_parciales)

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
