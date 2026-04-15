import json
import re
import time
import base64
from io import BytesIO
from typing import List, Dict, Optional

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
# CONFIGURACION
# =========================================================
st.set_page_config(page_title="Gestión de Bitácoras", layout="wide")
st.title("📋 Análisis Técnico de Terreno")

# Modelo gratuito recomendado actualmente en Gemini API
# Fallback por si cambia disponibilidad
MODEL_CANDIDATES = [
    "gemini-3.1-flash-lite-preview",
    "gemini-3-flash-preview",
    "gemini-1.5-flash",
]

# Limites conservadores para no agotar contexto/costos
MAX_ARCHIVOS_EN_POOL = 35
MAX_TEXT_CHARS_POR_ARCHIVO = 12000
MAX_TEXT_CHARS_POR_TANDA = 26000
MAX_ARCHIVOS_POR_TANDA = 6
MAX_PAGINAS_PDF = 8
MAX_FILAS_TABLA = 40

STOPWORDS = {
    "dame", "fotos", "serie", "estan", "carpeta", "numeros", "documentos",
    "archivos", "hizo", "que", "dia", "cómo", "como", "cual", "cuál",
    "sobre", "para", "desde", "hasta", "quiero", "necesito", "bitacora",
    "bitácora", "analiza", "analizar", "reporte", "informe"
}


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
    """
    Prueba modelos en orden hasta encontrar uno usable.
    """
    last_error = None
    for model_name in MODEL_CANDIDATES:
        try:
            llm = build_llm(model_name)
            # prueba corta
            llm.invoke([HumanMessage(content="Responde solo: OK")])
            return llm, model_name
        except Exception as e:
            last_error = e
            continue
    raise RuntimeError(f"No fue posible inicializar un modelo Gemini. Último error: {last_error}")


# =========================================================
# UTILIDADES
# =========================================================
def clean_text(text: str, max_chars: int = MAX_TEXT_CHARS_POR_ARCHIVO) -> str:
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


def escape_drive_query_value(value: str) -> str:
    # Evita romper query si la palabra contiene comillas simples
    return value.replace("'", "\\'")


def extract_keywords(user_input: str, max_keywords: int = 3) -> List[str]:
    palabras = re.findall(r"[\w-]+", user_input.lower())
    palabras = [p for p in palabras if len(p) > 3 and p not in STOPWORDS]

    if not palabras:
        palabras_crudas = re.findall(r"[\w-]+", user_input)
        if palabras_crudas:
            return [max(palabras_crudas, key=len)]
        return []

    # prioriza unicas y mas largas
    unicas = []
    for p in sorted(set(palabras), key=len, reverse=True):
        unicas.append(p)
    return unicas[:max_keywords]


def approx_size(item: Dict) -> int:
    contenido = item.get("contenido", "")
    if item.get("tipo") == "imagen":
        return 1500  # costo ficticio conservador
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

        # si el archivo individual ya es muy grande, lo dejamos solo
        if size >= max_chars_por_tanda:
            if actual:
                tandas.append(actual)
                actual = []
                chars_actuales = 0
            tandas.append([item])
            continue

        if (
            len(actual) >= max_archivos_por_tanda
            or chars_actuales + size > max_chars_por_tanda
        ):
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
    """
    Reintento con backoff exponencial simple.
    """
    last_error = None
    for intento in range(retries):
        try:
            return llm.invoke(prompt_or_messages)
        except Exception as e:
            last_error = e
            wait = base_wait * (2 ** intento)
            time.sleep(wait)
    raise last_error


# =========================================================
# BUSQUEDA DRIVE
# =========================================================
def buscar_archivos_drive(service, user_input: str) -> List[Dict]:
    palabras_clave = extract_keywords(user_input)
    pool_archivos = []
    seen_ids = set()

    if not palabras_clave:
        return []

    for termino in palabras_clave:
        termino = escape_drive_query_value(termino)
        q_files = (
            f"(name contains '{termino}' or fullText contains '{termino}') "
            f"and trashed = false"
        )

        try:
            files_out = (
                service.files()
                .list(
                    q=q_files,
                    fields="files(id, name, mimeType, modifiedTime)",
                    orderBy="modifiedTime desc",
                    pageSize=20,
                )
                .execute()
                .get("files", [])
            )

            for f in files_out:
                if f["id"] not in seen_ids:
                    pool_archivos.append(f)
                    seen_ids.add(f["id"])
        except Exception:
            continue

    return pool_archivos[:MAX_ARCHIVOS_EN_POOL]


# =========================================================
# LECTURA DE ARCHIVOS
# =========================================================
def descargar_archivo(service, file_id: str) -> BytesIO:
    request = service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh


def leer_pdf(fh: BytesIO) -> str:
    reader = PdfReader(fh)
    textos = []
    for page in reader.pages[:MAX_PAGINAS_PDF]:
        try:
            textos.append(page.extract_text() or "")
        except Exception:
            continue
    return clean_text(" ".join(textos))


def leer_excel_o_csv(fh: BytesIO, mime_type: str) -> str:
    try:
        if "csv" in mime_type:
            try:
                df = pd.read_csv(fh, nrows=MAX_FILAS_TABLA)
            except UnicodeDecodeError:
                fh.seek(0)
                df = pd.read_csv(fh, nrows=MAX_FILAS_TABLA, encoding="latin-1")
        else:
            df = pd.read_excel(fh, nrows=MAX_FILAS_TABLA)

        # limpia columnas muy anchas
        df = df.fillna("")
        texto = df.astype(str).head(MAX_FILAS_TABLA).to_string(index=False)
        return clean_text(texto)
    except Exception:
        return ""


def leer_imagen_base64(fh: BytesIO) -> Optional[str]:
    try:
        img = Image.open(fh).convert("RGB")
        img.thumbnail((900, 900))
        buffered = BytesIO()
        img.save(buffered, format="JPEG", quality=75)
        return base64.b64encode(buffered.getvalue()).decode("utf-8")
    except Exception:
        return None


def leer_archivo_multimodal(service, file_id, mime_type, file_name, fecha_mod):
    try:
        fh = descargar_archivo(service, file_id)
        fecha_legible = fecha_mod.split("T")[0]

        if "image" in mime_type:
            encoded = leer_imagen_base64(fh)
            if not encoded:
                return None
            return {
                "tipo": "imagen",
                "contenido": f"data:image/jpeg;base64,{encoded}",
                "nombre": file_name,
                "fecha": fecha_legible,
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
            }

        if (
            "spreadsheet" in mime_type
            or "csv" in mime_type
            or "excel" in mime_type
            or "sheet" in mime_type
        ):
            texto = leer_excel_o_csv(fh, mime_type)
            if not texto:
                return None
            return {
                "tipo": "texto",
                "contenido": texto,
                "nombre": file_name,
                "fecha": fecha_legible,
            }

        # texto plano / docs exportados como binario texto no manejado aquí
        return None

    except Exception:
        return None


# =========================================================
# PROMPTS
# =========================================================
def construir_prompt_resumen_tanda(user_input: str, items_tanda: List[Dict]) -> List[Dict]:
    instrucciones = f"""
Extrae datos técnicos útiles SOLO a partir del contenido entregado.

CONSULTA DEL USUARIO:
{user_input}

REGLAS:
- No inventes información.
- Si la consulta pregunta qué se hizo, interpreta cada archivo como una evidencia de actividad o hito.
- Resume por archivo.
- Incluye: nombre de archivo, fecha, pozo/activo si aparece, labor realizada, hallazgos técnicos, equipos/mediciones, observaciones.
- Si un archivo no aporta a la consulta, indícalo brevemente.
- Responde en formato estructurado y compacto.
""".strip()

    msg_content = [{"type": "text", "text": instrucciones}]

    for item in items_tanda:
        bloque = (
            f"\n\n=== ARCHIVO ===\n"
            f"Nombre: {item['nombre']}\n"
            f"Fecha: {item['fecha']}\n"
        )
        if item["tipo"] == "texto":
            bloque += f"Contenido:\n{item['contenido']}\n"
            msg_content[0]["text"] += bloque
        else:
            bloque += "Contenido: imagen adjunta\n"
            msg_content[0]["text"] += bloque
            msg_content.append(
                {"type": "image_url", "image_url": {"url": item["contenido"]}}
            )

    return msg_content


def construir_prompt_final(user_input: str, resumenes_parciales: List[str]) -> str:
    hallazgos = "\n\n".join(resumenes_parciales)

    return f"""
Genera un reporte final basado EXCLUSIVAMENTE en estos hallazgos parciales.

CONSULTA:
{user_input}

HALLAZGOS:
{hallazgos}

INSTRUCCIONES:
1. No uses introducciones de cortesía.
2. Organiza por pozo, activo o actividad técnica.
3. Incluye siempre el nombre del archivo fuente.
4. Si la consulta es sobre "qué se hizo", presenta los archivos como eventos o hitos cronológicos.
5. Si hay ambigüedad o faltan datos, dilo explícitamente.
6. Prioriza precisión por sobre redacción adornada.
""".strip()


# =========================================================
# FLUJO PRINCIPAL
# =========================================================
if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

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

            with st.spinner("Consultando Drive..."):
                archivos_totales = buscar_archivos_drive(service, user_input)

            if not archivos_totales:
                st.warning("No se encontraron registros relacionados.")
                st.stop()

            contenidos = []
            with st.spinner("Descargando y leyendo archivos..."):
                for f in archivos_totales:
                    res = leer_archivo_multimodal(
                        service,
                        f["id"],
                        f["mimeType"],
                        f["name"],
                        f["modifiedTime"],
                    )
                    if res:
                        contenidos.append(res)

            if not contenidos:
                st.warning("Se encontraron archivos, pero no se pudo extraer contenido útil.")
                st.stop()

            tandas = chunk_items_dinamicamente(contenidos)

            st.info(
                f"Se analizarán {len(contenidos)} archivos útiles en {len(tandas)} tanda(s)."
            )

            progress_bar = st.progress(0)
            resumenes_parciales = []

            for i, tanda in enumerate(tandas, start=1):
                with st.spinner(f"Analizando tanda {i}/{len(tandas)}..."):
                    msg_content = construir_prompt_resumen_tanda(user_input, tanda)
                    try:
                        resp_tanda = safe_invoke(
                            llm, [HumanMessage(content=msg_content)], retries=4
                        )
                        if resp_tanda and getattr(resp_tanda, "content", None):
                            resumenes_parciales.append(resp_tanda.content)
                    except Exception as e:
                        resumenes_parciales.append(
                            f"[Tanda {i} no procesada por error: {str(e)}]"
                        )

                progress_bar.progress(i / len(tandas))

            if not resumenes_parciales:
                st.error("No fue posible generar resúmenes parciales.")
                st.stop()

            with st.spinner("Consolidando reporte final..."):
                prompt_final = construir_prompt_final(user_input, resumenes_parciales)
                respuesta_final = safe_invoke(
                    llm,
                    [HumanMessage(content=prompt_final)],
                    retries=4
                )

            contenido_final = respuesta_final.content
            st.markdown(contenido_final)
            st.session_state.messages.append(
                {"role": "assistant", "content": contenido_final}
            )

        except Exception as e:
            st.error(f"Error técnico: {str(e)}")
