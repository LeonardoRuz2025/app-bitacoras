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
            # Bajamos a 700px para ahorrar tokens de visión
            img = Image.open(fh).convert('RGB')
            img.thumbnail((700, 700)) 
            buffered = BytesIO()
            img.save(buffered, format="JPEG", quality=75)
            encoded = base64.b64encode(buffered.getvalue()).decode('utf-8')
            return {"tipo": "imagen", "contenido": f"data:image/jpeg;base64,{encoded}"}
            
        elif mime_type == 'application/pdf':
            # Extraemos texto de forma más compacta
            reader = PdfReader(fh)
            texto = ""
            for page in reader.pages[:10]: # Limitamos a las primeras 10 páginas por doc
                texto += page.extract_text() + "\n"
            return {"tipo": "texto", "contenido": texto}
        elif 'spreadsheet' in mime_type or 'csv' in mime_type or 'excel' in mime_type:
            df = pd.read_excel(fh) if 'spreadsheet' in mime_type else pd.read_csv(fh)
            return {"tipo": "texto", "contenido": df.head(50).to_string()} # Solo primeras 50 filas
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
        
        pool_archivos = []
        seen_ids = set()
        
        # Extracción de fecha para búsqueda cronológica
        regex_fecha = re.search(r'(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)', user_input.lower())
        
        with st.spinner("Rastreando registros técnicos..."):
            # Prioridad 1: Búsqueda por fecha si existe
            if regex_fecha:
                # Ajustamos la ventana de tiempo (8 de abril de 2026)
                q_time = "modifiedTime > '2026-04-07T00:00:00Z' and modifiedTime < '2026-04-09T23:59:59Z' and trashed = false"
                files_time = service.files().list(q=q_time, fields="files(id, name, mimeType)").execute().get('files', [])
                for f in files_time:
                    if f['id'] not in seen_ids:
                        pool_archivos.append(f)
                        seen_ids.add(f['id'])

            # Prioridad 2: Palabras clave técnicas
            palabras_tecnicas = ["PTS", "DGA", "Sensor", "Bitacora", "Reporte"]
            # Extraemos palabras de la pregunta
            palabras_usuario = [p for p in re.findall(r'[\w-]+', user_input) if len(p) > 3]
            busqueda = (palabras_usuario + palabras_tecnicas)[:6]

            for t in busqueda:
                q_files = f"name contains '{t}' and trashed = false"
                files_out = service.files().list(q=q_files, fields="files(id, name, mimeType)").execute().get('files', [])
                for f in files_out:
                    if f['id'] not in seen_ids:
                        pool_archivos.append(f)
                        seen_ids.add(f['id'])

        # --- FILTRO DE CALIDAD Y CUOTA ---
        # Penalizamos "LIDERAZGO" y "MANUAL" para que queden al final si hay mucho archivo
        pool_archivos = sorted(pool_archivos, key=lambda x: ("LIDERAZGO" in x['name'].upper() or "PLAN" in x['name'].upper()))
        
        # REDUCIMOS EL LÍMITE A 12 ARCHIVOS PARA NO QUEMAR LA CUOTA
        archivos_a_procesar = pool_archivos[:12]
        
        if not archivos_a_procesar:
            st.warning("No se encontraron registros que coincidan.")
            st.stop()

        textos_extraidos = ""
        imagenes_base64 = []
        
        st.info(f"Analizando los {len(archivos_a_procesar)} registros más relevantes...")
        
        bar = st.progress(0)
        for i, f in enumerate(archivos_a_procesar):
            res = leer_archivo_multimodal(service, f['id'], f['mimeType'], f['name'])
            if res:
                if res["tipo"] == "texto":
                    textos_extraidos += f"\n--- ORIGEN: {f['name']} ---\n{res['contenido']}\n"
                elif res["tipo"] == "imagen":
                    imagenes_base64.append({"url": res["contenido"], "nombre": f['name']})
            bar.progress((i + 1) / len(archivos_a_procesar))
            
        # Cortafuegos estricto de texto
        if len(textos_extraidos) > 50000:
            textos_extraidos = textos_extraidos[:50000] + "...[RESUMIDO]"

        with st.spinner("Generando reporte..."):
            # Cambiamos a 1.5-flash que es más generoso con las cuotas
            llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", google_api_key=st.secrets["GEMINI_API_KEY"])
            
            prompt_maestro = f"""
            Analiza los documentos y fotos para reconstruir los hechos.
            CONSULTA: "{user_input}"
            
            DATOS DE CAMPO:
            {textos_extraidos}
            
            REGLAS:
            1. Reporte directo sin introducciones.
            2. Prioriza hitos reales: visitas DGA, toma de datos, cambios de sensores, folios PTS, nombres de personal.
            3. Si un archivo es solo un "Plan" o "Liderazgo", menciónalo solo si no hay bitácoras reales.
            4. Organiza por pozo o actividad técnica.
            5. Indica el archivo de origen para cada dato.
            """
            
            mensaje_contenido = [{"type": "text", "text": prompt_maestro}]
            for img in imagenes_base64:
                mensaje_contenido.append({"type": "image_url", "image_url": {"url": img["url"]}})
                
            try:
                response = llm.invoke([HumanMessage(content=mensaje_contenido)])
                st.markdown(response.content)
                st.session_state.messages.append({"role": "assistant", "content": response.content})
            except Exception as e:
                st.error(f"Límite de capacidad alcanzado. Intente con una pregunta más específica o espere 30 segundos. Detalle: {str(e)}")
