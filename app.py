import streamlit as st
import re
import time
import pandas as pd
import base64
from io import BytesIO
from PIL import Image
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage
from pypdf import PdfReader

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Analista Total de Terreno", layout="wide")
st.title("🚜 Analista de Carpeta Completa (Groq Llama 4)")

def get_drive_service():
    creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    return build('drive', 'v3', credentials=creds)

# --- FUNCIÓN LECTORA INDIVIDUAL ---
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
    except Exception: return None
    return None

# --- INTERFAZ ---
if "messages" not in st.session_state: st.session_state.messages = []
for m in st.session_state.messages:
    with st.chat_message(m["role"]): st.markdown(m["content"])

user_input = st.chat_input("Ej: Dame los números de serie de las fotos de la carpeta PBPC-06")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"): st.markdown(user_input)

    with st.chat_message("assistant"):
        service = get_drive_service()
        
        # 1. BÚSQUEDA INTELIGENTE DE CARPETAS Y ARCHIVOS
        palabras_crudas = re.findall(r'[\w-]+', user_input)
        palabras_clave = [p for p in palabras_crudas if len(p) > 3 and p.lower() not in ['dame', 'fotos', 'serie', 'estan', 'carpeta', 'numeros']]
        if not palabras_clave: palabras_clave = [max(palabras_crudas, key=len)]
            
        pool_archivos = []
        seen_ids = set()
        
        with st.spinner("Rastreando carpetas y archivos en Drive..."):
            for t in palabras_clave:
                # Paso A: Buscar carpetas que se llamen como el pozo
                q_folder = f"name contains '{t}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
                folders = service.files().list(q=q_folder).execute().get('files', [])
                
                for folder in folders:
                    # Traer TODOS los archivos dentro de esa carpeta
                    q_in_folder = f"'{folder['id']}' in parents and trashed = false"
                    files_in = service.files().list(q=q_in_folder).execute().get('files', [])
                    for f in files_in:
                        if f['id'] not in seen_ids:
                            pool_archivos.append(f)
                            seen_ids.add(f['id'])
                
                # Paso B: Buscar archivos sueltos por nombre o texto (backup)
                q_files = f"(name contains '{t}' or fullText contains '{t}') and trashed = false"
                files_out = service.files().list(q=q_files).execute().get('files', [])
                for f in files_out:
                    if f['id'] not in seen_ids:
                        pool_archivos.append(f)
                        seen_ids.add(f['id'])

        pool_fotos = [f for f in pool_archivos if 'image' in f['mimeType']]
        
        if not pool_fotos:
            st.warning("No encontré fotos en esa carpeta o con ese nombre.")
        else:
            llm = ChatGroq(model="meta-llama/llama-4-scout-17b-16e-instruct", groq_api_key=st.secrets["GROQ_API_KEY"])
            hallazgos = []
            
            total = len(pool_fotos)
            st.success(f"¡He localizado {total} fotos! Iniciando análisis exhaustivo...")
            
            # 2. PROCESAR SECUENCIALMENTE (Sin límite de 4, vamos por todas)
            for i, f in enumerate(pool_fotos):
                progress_text = f"Analizando foto {i+1} de {total}: {f['name']}"
                with st.spinner(progress_text):
                    res = leer_archivo_multimodal(service, f['id'], f['mimeType'], f['name'])
                    
                    if res and res["tipo"] == "imagen":
                        prompt = f"Analiza esta imagen de terreno. Pregunta: {user_input}. Si ves números de serie, marcas o datos técnicos, escríbelos. Si no ves nada relevante, responde 'Sin datos técnicos'."
                        
                        try:
                            resp = llm.invoke([HumanMessage(content=[
                                {"type": "text", "text": prompt},
                                {"type": "image_url", "image_url": {"url": res["contenido"]}}
                            ])])
                            if "Sin datos técnicos" not in resp.content:
                                hallazgos.append(f"✅ **{f['name']}**: {resp.content}")
                        except Exception as e:
                            hallazgos.append(f"❌ **{f['name']}**: Error en servidor.")
                        
                        # Pausa para resetear tokens de Groq
                        if i < total - 1:
                            time.sleep(12)
            
            # 3. RESULTADO
            if hallazgos:
                st.markdown("### 📊 Información Extraída de las Fotos:")
                for h in hallazgos: st.markdown(h)
            else:
                st.info("Revisé todas las fotos pero no logré distinguir números de serie claros.")

            st.session_state.messages.append({"role": "assistant", "content": f"Análisis de {total} fotos completado."})
