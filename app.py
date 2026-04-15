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
st.set_page_config(page_title="Reporte Técnico de Terreno", layout="wide")
st.title("📋 Gestión de Bitácoras Técnicas")

def get_drive_service():
    # Leemos el JSON completo desde un solo secreto
    info_claves = json.loads(st.secrets["GOOGLE_JSON_COMPLETO"])
    
    # Creamos las credenciales
    creds = service_account.Credentials.from_service_account_info(info_claves)
    return build('drive', 'v3', credentials=creds)
    
# --- FUNCIÓN LECTORA ---
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
            # Comprimimos la imagen a 800px para ahorrar tokens valiosos
            img = Image.open(fh).convert('RGB')
            img.thumbnail((800, 800)) 
            buffered = BytesIO()
            img.save(buffered, format="JPEG", quality=80)
            encoded = base64.b64encode(buffered.getvalue()).decode('utf-8')
            return {"tipo": "imagen", "contenido": f"data:image/jpeg;base64,{encoded}"}
            
        elif mime_type == 'application/pdf':
            return {"tipo": "texto", "contenido": " ".join([p.extract_text() for p in PdfReader(fh).pages])}
        elif 'spreadsheet' in mime_type or 'csv' in mime_type or 'excel' in mime_type:
            df = pd.read_excel(fh) if 'spreadsheet' in mime_type else pd.read_csv(fh)
            return {"tipo": "texto", "contenido": df.to_string()}
    except Exception: 
        return None
    return None

# --- INTERFAZ ---
if "messages" not in st.session_state: 
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]): 
        st.markdown(m["content"])

user_input = st.chat_input("Consulte sobre pozos, fechas o actividades técnicas...")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"): 
        st.markdown(user_input)

    with st.chat_message("assistant"):
        service = get_drive_service()
        
        # 1. BÚSQUEDA INTELIGENTE
        palabras_crudas = re.findall(r'[\w-]+', user_input)
        palabras_clave = [p for p in palabras_crudas if len(p) > 3 and p.lower() not in ['dame', 'fotos', 'serie', 'estan', 'carpeta', 'numeros', 'documentos', 'archivos']]
        if not palabras_clave: 
            palabras_clave = [max(palabras_crudas, key=len)]
            
        pool_archivos = []
        seen_ids = set()
        
        with st.spinner("Localizando registros en Drive..."):
            for t in palabras_clave:
                q_folder = f"name contains '{t}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
                folders = service.files().list(q=q_folder).execute().get('files', [])
                
                for folder in folders:
                    q_in_folder = f"'{folder['id']}' in parents and trashed = false"
                    files_in = service.files().list(q=q_in_folder).execute().get('files', [])
                    for f in files_in:
                        if f['id'] not in seen_ids:
                            pool_archivos.append(f)
                            seen_ids.add(f['id'])
                
                q_files = f"(name contains '{t}' or fullText contains '{t}') and trashed = false"
                files_out = service.files().list(q=q_files).execute().get('files', [])
                for f in files_out:
                    if f['id'] not in seen_ids:
                        pool_archivos.append(f)
                        seen_ids.add(f['id'])

        # SEPARACIÓN Y CORTAFUEGOS
        pool_fotos = [f for f in pool_archivos if 'image' in f['mimeType']][:25] 
        pool_documentos = [f for f in pool_archivos if 'image' not in f['mimeType']][:10] 
        archivos_a_procesar = pool_fotos + pool_documentos
        
        if not archivos_a_procesar:
            st.warning("No se encontró información que coincida con la búsqueda.")
            st.stop()

        # 2. DESCARGA Y PREPARACIÓN
        textos_extraidos = ""
        imagenes_base64 = []
        
        st.info(f"Procesando {len(archivos_a_procesar)} archivos relevantes...")
        
        bar = st.progress(0)
        for i, f in enumerate(archivos_a_procesar):
            res = leer_archivo_multimodal(service, f['id'], f['mimeType'], f['name'])
            if res:
                if res["tipo"] == "texto":
                    textos_extraidos += f"\n--- Archivo: {f['name']} ---\n{res['contenido']}\n"
                elif res["tipo"] == "imagen":
                    imagenes_base64.append({"url": res["contenido"], "nombre": f['name']})
            bar.progress((i + 1) / len(archivos_a_procesar))
            
        if len(textos_extraidos) > 80000:
            textos_extraidos = textos_extraidos[:80000] + "\n...[CONTENIDO TRUNCADO]"

        # 3. ENVÍO SEGURO A GEMINI
        with st.spinner("Analizando evidencia..."):
            llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", google_api_key=st.secrets["GEMINI_API_KEY"])
            
            prompt_maestro = f"""
            INSTRUCCIONES DE RESPUESTA:
            - Responde de forma DIRECTA, técnica y concisa.
            - ESTÁ PROHIBIDO usar introducciones como "Como Ingeniero experto", "Entiendo tu pregunta" o presentaciones similares. Ve directamente a la información.
            - Si la consulta es sobre "labores", "actividades", "tareas" o "qué se hizo" en una fecha específica, interpreta cada archivo encontrado (documento, foto o tabla) de ese día como evidencia de un hecho, acto o evento realizado.
            - Describe los hallazgos basándote estrictamente en la evidencia de los archivos adjuntos.
            
            PREGUNTA DEL USUARIO: "{user_input}"
            
            TEXTOS ENCONTRADOS:
            {textos_extraidos}
            
            REQUISITOS TÉCNICOS:
            1. Analiza TODA la información proporcionada (textos y fotos).
            2. Identifica el pozo u objetivo específico y descarta información de otros pozos.
            3. Cita obligatoriamente el nombre del archivo de origen para cada hito o dato mencionado.
            4. Si la información es insuficiente para responder, indícalo de forma breve.
            """
            
            mensaje_contenido = [{"type": "text", "text": prompt_maestro}]
            for img in imagenes_base64:
                mensaje_contenido.append({"type": "image_url", "image_url": {"url": img["url"]}})
                
            try:
                response = llm.invoke([HumanMessage(content=mensaje_contenido)])
                respuesta_final = response.content
            except Exception as e:
                respuesta_final = f"Error en la consulta: {str(e)}"
                
        st.markdown(respuesta_final)
        st.session_state.messages.append({"role": "assistant", "content": respuesta_final})
