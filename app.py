import json
import streamlit as st
import re
import pandas as pd
import base64
from io import BytesIO
from PIL import Image
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
from pypdf import PdfReader
from datetime import datetime

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Sistema de Análisis de Bitácoras", layout="wide")
st.title("📋 Análisis Técnico de Bitácoras y Terreno")

def get_drive_service():
    info_claves = json.loads(st.secrets["GOOGLE_JSON_COMPLETO"])
    creds = service_account.Credentials.from_service_account_info(info_claves)
    return build('drive', 'v3', credentials=creds)
    
def leer_archivo_multimodal(service, file_id, mime_type, file_name):
    try:
        request = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        
        if 'image' in mime_type:
            img = Image.open(fh).convert('RGB')
            img.thumbnail((800, 800)) 
            buffered = BytesIO()
            img.save(buffered, format="JPEG", quality=80)
            encoded = base64.b64encode(buffered.getvalue()).decode('utf-8')
            return {"tipo": "imagen", "contenido": f"data:image/jpeg;base64,{encoded}"}
            
        elif mime_type == 'application/pdf':
            return {"tipo": "texto", "contenido": " ".join([p.extract_text() for p in PdfReader(fh).pages])}
        elif 'spreadsheet' in mime_type or 'csv' in mime_type or 'excel' in mime_type:
            # Cargamos solo las primeras filas para no saturar si es muy grande
            df = pd.read_excel(fh) if 'spreadsheet' in mime_type else pd.read_csv(fh)
            return {"tipo": "texto", "contenido": df.head(100).to_string()}
    except Exception: 
        return None
    return None

# --- INTERFAZ ---
if "messages" not in st.session_state: 
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]): 
        st.markdown(m["content"])

user_input = st.chat_input("Ej: ¿Qué se hizo el 8 de abril en los pozos?")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"): 
        st.markdown(user_input)

    with st.chat_message("assistant"):
        service = get_drive_service()
        
        # --- 1. DETECCIÓN DE FECHA Y BÚSQUEDA AVANZADA ---
        pool_archivos = []
        seen_ids = set()
        
        # Intentamos extraer una fecha del texto (formatos comunes)
        regex_fecha = re.search(r'(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)', user_input.lower())
        
        with st.spinner("Realizando rastreo exhaustivo en Drive..."):
            # Si hay fecha, buscamos por metadatos de tiempo (Esto es lo que hace Drive Gemini)
            if regex_fecha:
                # Si el usuario pregunta por el 8 de abril de 2026:
                # Buscamos archivos modificados en esa ventana de tiempo
                q_time = "modifiedTime > '2026-04-07T00:00:00Z' and modifiedTime < '2026-04-09T23:59:59Z' and trashed = false"
                files_time = service.files().list(q=q_time, fields="files(id, name, mimeType)").execute().get('files', [])
                for f in files_time:
                    if f['id'] not in seen_ids:
                        pool_archivos.append(f)
                        seen_ids.add(f['id'])

            # Búsqueda por palabras clave técnicas (agregamos términos de terreno)
            palabras_técnicas = ["PTS", "DGA", "Sensor", "Pozo", "Bitacora", "Reporte", "Visita"]
            palabras_usuario = re.findall(r'[\w-]+', user_input)
            palabras_clave = [p for p in palabras_usuario if len(p) > 3] + palabras_técnicas

            for t in palabras_clave[:5]: # Limitamos para no saturar la API
                q_files = f"(name contains '{t}' or fullText contains '{t}') and trashed = false"
                files_out = service.files().list(q=q_files, fields="files(id, name, mimeType)").execute().get('files', [])
                for f in files_out:
                    if f['id'] not in seen_ids:
                        pool_archivos.append(f)
                        seen_ids.add(f['id'])

        # Priorizar archivos que NO sean "Liderazgo" o "Manuales" si hay muchos
        pool_archivos = sorted(pool_archivos, key=lambda x: ("LIDERAZGO" in x['name'].upper() or "MANUAL" in x['name'].upper()))
        
        archivos_a_procesar = pool_archivos[:20] # Procesamos un poco más de contexto
        
        if not archivos_a_procesar:
            st.warning("No se encontraron registros técnicos para los términos o la fecha indicada.")
            st.stop()

        # 2. DESCARGA Y PREPARACIÓN
        textos_extraidos = ""
        imagenes_base64 = []
        
        st.info(f"Analizando {len(archivos_a_procesar)} documentos y registros técnicos encontrados...")
        
        bar = st.progress(0)
        for i, f in enumerate(archivos_a_procesar):
            res = leer_archivo_multimodal(service, f['id'], f['mimeType'], f['name'])
            if res:
                if res["tipo"] == "texto":
                    textos_extraidos += f"\n--- ORIGEN: {f['name']} ---\n{res['contenido']}\n"
                elif res["tipo"] == "imagen":
                    imagenes_base64.append({"url": res["contenido"], "nombre": f['name']})
            bar.progress((i + 1) / len(archivos_a_procesar))
            
        # 3. ENVÍO A GEMINI CON PROMPT DE AUDITORÍA
        with st.spinner("Generando reporte técnico detallado..."):
            llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", google_api_key=st.secrets["GEMINI_API_KEY"])
            
            prompt_maestro = f"""
            Actúa como un Auditor Técnico de Terreno. Tu objetivo es reconstruir los hechos ocurridos según la consulta.
            CONSULTA: "{user_input}"
            
            DATOS DISPONIBLES:
            {textos_extraidos}
            
            INSTRUCCIONES DE RESPUESTA (ESTRICTO):
            1. Prohibido usar introducciones. Entrega un reporte directo.
            2. PRIORIDAD DE DATOS: Si encuentras reportes de campo, correos de solicitud de ingreso, cambios de sensores (series), visitas de la DGA o permisos PTS, dales prioridad absoluta sobre planes mensuales o documentos de "Liderazgo".
            3. Si la pregunta es sobre una fecha: identifica horas, nombres de pozos (ej. PBS-05, PBO-10), personas (ej. Samuel Huanchicay) y cambios técnicos.
            4. Si un documento es un "Plan Mensual" y solo dice lo que "debería" hacerse, aclara que es una programación, no un hecho realizado.
            5. Organiza la respuesta por hitos o pozos visitados.
            6. Indica el nombre del archivo de origen para cada hito mencionado.
            """
            
            mensaje_contenido = [{"type": "text", "text": prompt_maestro}]
            for img in imagenes_base64:
                mensaje_contenido.append({"type": "image_url", "image_url": {"url": img["url"]}})
                
            try:
                response = llm.invoke([HumanMessage(content=mensaje_contenido)])
                respuesta_final = response.content
            except Exception as e:
                respuesta_final = f"Error en la generación del reporte: {str(e)}"
                
        st.markdown(respuesta_final)
        st.session_state.messages.append({"role": "assistant", "content": respuesta_final})
