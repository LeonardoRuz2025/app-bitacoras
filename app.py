import json
import re
import time
import base64
from io import BytesIO
from typing import List, Dict, Optional, Any, Tuple

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

MAX_ARCHIVOS_EN_POOL = 45
MAX_TEXT_CHARS_POR_ARCHIVO = 12000
MAX_TEXT_CHARS_POR_TANDA = 26000
MAX_ARCHIVOS_POR_TANDA = 6
MAX_PAGINAS_PDF = 8
MAX_FILAS_TABLA = 50
TOP_RESULTADOS_POR_QUERY = 20

STOPWORDS = {
    "dame", "quiero", "necesito", "podrias", "podrías", "puedes",
    "consulta", "pregunta", "sobre", "para", "desde", "hasta", "entre",
    "cuando", "cuándo", "ultima", "última", "ultimo", "último",
    "vez", "registro", "registros", "bitacora", "bitácora",
    "documentos", "archivos", "carpeta", "carpetas", "drive",
    "pozo", "pozos", "que", "qué", "cual", "cuál", "como", "cómo",
    "del", "los", "las", "una", "unos", "unas", "ese", "esa",
    "dia", "días", "día", "dias", "instalados", "instalado"
}

MIME_TEXT = {
    "application/pdf",
    "text/csv",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}

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
            test = llm.invoke([HumanMessage(content="Responde solo OK")])
            text = normalizar_respuesta_llm(getattr(test, "content", ""))
            if text:
                return llm, model_name
        except Exception as e:
            last_error = e
    raise RuntimeError(f"No fue posible inicializar Gemini. Último error: {last_error}")


# =========================================================
# UTILIDADES
# =========================================================
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


def parse_date_text(user_input: str) -> Optional[str]:
    # formatos como 2025-03-01, 01-03-2025, 01/03/2025
    patterns = [
        r"\b(\d{4}-\d{2}-\d{2})\b",
        r"\b(\d{2}/\d{2}/\d{4})\b",
        r"\b(\d{2}-\d{2}-\d{4})\b",
    ]
    for p in patterns:
        m = re.search(p, user_input)
        if m:
            return m.group(1)
    return None


def detect_well_codes(text: str) -> List[str]:
    # Ejemplos: PBO-08, PBPC-01, PBO08, PBPC 01
    found = set()

    patterns = [
        r"\b([A-Z]{2,6}-\d{1,3}[A-Z]?)\b",
        r"\b([A-Z]{2,6}\s\d{1,3}[A-Z]?)\b",
        r"\b([A-Z]{2,6}\d{1,3}[A-Z]?)\b",
    ]

    upper_text = text.upper()
    for pattern in patterns:
        for m in re.findall(pattern, upper_text):
            code = re.sub(r"\s+", "-", m.strip())
            found.add(code)

    return list(found)


def build_code_variants(code: str) -> List[str]:
    code = code.upper().strip()
    variants = {
        code,
        code.replace("-", " "),
        code.replace("-", ""),
    }
    return [v for v in variants if v]


def extract_keywords(user_input: str, max_keywords: int = 4) -> List[str]:
    words = re.findall(r"[\w-]+", user_input.lower())
    words = [w for w in words if len(w) > 2 and w not in STOPWORDS]

    unique_words = []
    for w in sorted(set(words), key=len, reverse=True):
        unique_words.append(w)

    return unique_words[:max_keywords]


def classify_query(user_input: str) -> Dict[str, bool]:
    text = user_input.lower()

    serial_terms = [
        "serial", "serie", "número de serie", "numero de serie", "sn", "s/n",
        "placa", "etiqueta", "sticker", "modelo del sensor", "seriales"
    ]
    activity_terms = [
        "que se hizo", "qué se hizo", "actividades", "labores", "trabajos",
        "faenas", "tareas", "hitos", "intervenciones", "realizado", "realizadas"
    ]
    latest_terms = [
        "ultima vez", "última vez", "último registro", "ultimo registro",
        "último", "ultimo", "más reciente", "mas reciente", "último reporte"
    ]
    install_terms = [
        "instalado", "instalados", "instalada", "instaladas",
        "equipos", "sensores", "instrumentos"
    ]

    return {
        "seriales": any(t in text for t in serial_terms),
        "actividades": any(t in text for t in activity_terms),
        "ultimos_registros": any(t in text for t in latest_terms),
        "instalacion": any(t in text for t in install_terms),
        "fecha_especifica": parse_date_text(user_input) is not None,
    }


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


def safe_invoke(llm, prompt_or_messages, retries: int = 4, base_wait: float = 2.0):
    last_error = None
    for intento in range(retries):
        try:
            return llm.invoke(prompt_or_messages)
        except Exception as e:
            last_error = e
            time.sleep(base_wait * (2 ** intento))
    raise last_error


# =========================================================
# BUSQUEDA EN DRIVE
# =========================================================
def build_search_terms(user_input: str) -> List[str]:
    terms = []
    query_type = classify_query(user_input)

    well_codes = detect_well_codes(user_input)
    for code in well_codes:
        terms.extend(build_code_variants(code))

    date_text = parse_date_text(user_input)
    if date_text:
        terms.append(date_text)

    keywords = extract_keywords(user_input)
    terms.extend(keywords)

    # si no hay nada, usar texto entero resumido
    if not terms:
        terms.append(user_input.strip())

    # quitar duplicados conservando orden
    unique = []
    seen = set()
    for t in terms:
        key = t.lower().strip()
        if key and key not in seen:
            unique.append(t)
            seen.add(key)

    # si es consulta de seriales o actividades, conviene priorizar pozo y sensor
    if query_type["seriales"]:
        for extra in ["sensor", "sensores", "serial", "serie", "equipo", "instalado"]:
            if extra not in seen:
                unique.append(extra)

    if query_type["actividades"]:
        for extra in ["bitacora", "terreno", "visita", "trabajo", "actividad"]:
            if extra not in seen:
                unique.append(extra)

    return unique[:8]


def list_files_for_term(service, term: str) -> List[Dict]:
    term = escape_drive_query_value(term)

    # Busca en nombre y contenido indexado, en todo Drive accesible por la cuenta
    q = (
        f"(name contains '{term}' or fullText contains '{term}') "
        f"and trashed = false"
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
        return resp.get("files", [])
    except Exception:
        return []


def buscar_archivos_drive(service, user_input: str) -> List[Dict]:
    terms = build_search_terms(user_input)
    pool = []
    seen_ids = set()

    for term in terms:
        files = list_files_for_term(service, term)
        for f in files:
            if f["id"] not in seen_ids:
                pool.append(f)
                seen_ids.add(f["id"])

    def score_file(f: Dict) -> Tuple[int, str]:
        score = 0
        name = f.get("name", "").lower()
        q = user_input.lower()

        for code in detect_well_codes(user_input):
            for variant in build_code_variants(code):
                if variant.lower() in name:
                    score += 10

        if "pdf" in name:
            score += 1

        if any(k in q for k in ["serial", "serie", "placa", "etiqueta", "sensor"]):
            if "foto" in name or "img" in name or "image" in name or "sensor" in name:
                score += 3

        return score, f.get("modifiedTime", "")

    pool.sort(key=lambda x: score_file(x), reverse=True)
    return pool[:MAX_ARCHIVOS_EN_POOL]


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
# LECTURA DE CONTENIDO
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

        if "image" in mime_type:
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
Analiza los archivos entregados y responde SOLO a partir de la evidencia disponible en los documentos e imágenes.

CONSULTA DEL USUARIO:
{user_input}

CONTEXTO OPERATIVO:
- Los archivos pertenecen a un Drive de bitácoras técnicas de terreno.
- Puede haber registros diarios de trabajos en pozos.
- Las imágenes también son evidencia válida.
- Cada archivo tiene nombre y fecha de modificación.
- Debes usar tanto el texto extraído como la inspección visual de las imágenes.

REGLAS OBLIGATORIAS:
1. No inventes información.
2. Usa únicamente evidencia encontrada en estos archivos.
3. Si algo no es legible o no se puede confirmar, indícalo explícitamente.
4. Siempre cita el nombre del archivo fuente y su fecha.
5. Si hay imágenes, inspecciona visualmente etiquetas, placas, instrumentos, sensores, tableros y textos visibles.
6. Si la pregunta pide seriales, extrae SOLO seriales claramente visibles o explícitos en el texto.
7. Si la pregunta pide actividades de un día, trata cada archivo del día como evidencia de actividad realizada.
8. Si hay conflicto entre archivos, menciónalo.
9. Si un archivo no aporta a la consulta, dilo brevemente.
10. Responde de forma estructurada, compacta y técnica.
""".strip()

    reglas_especificas = []

    if query_flags["seriales"]:
        reglas_especificas.append("""
CASO ESPECIAL: SERIALes / PLACAS / ETIQUETAS
- Busca números de serie, S/N, SN, códigos de equipo, modelos, etiquetas o placas.
- Si un serial aparece incompleto o borroso, márcalo como "dudoso" o "parcial".
- No completes caracteres faltantes.
- Indica sensor/equipo asociado y nivel de confianza: alta, media o baja.
""".strip())

    if query_flags["actividades"] or query_flags["fecha_especifica"]:
        reglas_especificas.append(f"""
CASO ESPECIAL: ACTIVIDADES / BITÁCORA DIARIA
- Si la consulta es sobre qué se hizo, describe actividades, labores, mediciones, instalaciones, inspecciones o hallazgos.
- Trata cada archivo como evidencia de un evento técnico.
- Organiza cronológicamente cuando sea posible.
- Si el usuario menciona una fecha ({fecha_texto if fecha_texto else "sin fecha explícita"}), prioriza evidencia de ese día.
""".strip())

    if query_flags["ultimos_registros"]:
        reglas_especificas.append("""
CASO ESPECIAL: ÚLTIMO REGISTRO
- Determina cuál es la evidencia más reciente relacionada con la consulta según la fecha del archivo.
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
""".strip()
    ]

    if query_flags["seriales"]:
        instrucciones.append("""
FORMATO RECOMENDADO PARA SERIALes:
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
- Actividad / hito
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
# INTERFAZ PRINCIPAL
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
                for f in archivos_totales[:20]:
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
