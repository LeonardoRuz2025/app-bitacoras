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
st.set_page_config(page_title="Analista Secuencial Groq", layout="wide")
st.title("👁️🧠 Analista de Terreno (Modo Secuencial Groq)")

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
            img.thumbnail((600, 600)) # Compresión estricta
            buffered = BytesIO()
            img.save(buffered, format="JPEG", quality=70)
            encoded = base64.b64encode(buffered.getvalue()).decode('utf-8')
            return {"tipo": "imagen", "contenido": f"data:image/jpeg;base64,{encoded}"}
            
        elif mime_type == 'application/pdf':
            return {"tipo": "texto", "contenido": " ".join([p.extract_text() for p in PdfReader(fh).pages])}
        elif 'spreadsheet' in mime_type or 'csv' in mime_type:
            df = pd.read_excel(fh) if 'spreadsheet' in mime_type else pd.read_csv(fh)
            return {"tipo": "texto", "contenido": df.to_string()}
    except Exception: return None
    return None

# --- INTERFAZ ---
if "messages" not in st.session_state: st.session_state.messages = []
for m in st.session_state.messages:
    with st.chat_message(m["role"]): st.markdown(m["content"])

user_input = st.chat_input("Dime los números de serie del PBPC-06 en las fotos...")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"): st.markdown(user_input)

    with st.chat_message("assistant"):
        service = get_drive_service()
        
        # 1. BUSCAR ARCHIVOS
        palabras_crudas = re.findall(r'[\w-]+', user_input)
        palabras_clave = [p for p in palabras_crudas if len(p) > 3 and p.lower() not in ['dame', 'fotos', 'los', 'del', 'las', 'numeros', 'serie', 'estan']]
        if not palabras_clave: palabras_clave = [max(palabras_crudas, key=len)]
            
        todos_los_resultados = []
        seen_ids = set()
        with st.spinner("Buscando archivos en Drive..."):
            for t in palabras_clave:
                q = f"fullText contains '{t}' and trashed = false"
                results = service.files().list(q=q, fields="files(id, name, mimeType)").execute()
                for f in results.get('files', []):
                    if f['id'] not in seen_ids:
                        todos_los_resultados.append(f)
                        seen_ids.add(f['id'])

        pool_fotos = [f for f in todos_los_resultados if 'image' in f['mimeType']]
        
        if not pool_fotos:
            st.warning("No encontré fotografías asociadas a tu búsqueda en Drive.")
            st.session_state.messages.append({"role": "assistant", "content": "No encontré fotos."})
        else:
            # Inicializamos Llama 4 Scout
            llm = ChatGroq(model="meta-llama/llama-4-scout-17b-16e-instruct", groq_api_key=st.secrets["GROQ_API_KEY"])
            
            hallazgos = []
            st.success(f"¡Se encontraron {len(pool_fotos)} fotos! Analizándolas una por una...")
            
            # 2. PROCESAR FOTOS SECUENCIALMENTE (Máximo 4 fotos para evitar Timeouts largos)
            for i, f in enumerate(pool_fotos[:4]):
                with st.spinner(f"👁️ Analizando foto {i+1}: {f['name']}..."):
                    res = leer_archivo_multimodal(service, f['id'], f['mimeType'], f['name'])
                    
                    if res and res["tipo"] == "imagen":
                        prompt_foto = f"Analiza esta foto. PREGUNTA: {user_input}. Si ves la respuesta (ej. un número de serie), dímela. Si no la ves, responde 'No encontrado en esta foto'."
                        mensaje = [
                            {"type": "text", "text": prompt_foto},
                            {"type": "image_url", "image_url": {"url": res["contenido"]}}
                        ]
                        
                        try:
                            respuesta_parcial = llm.invoke([HumanMessage(content=mensaje)])
                            hallazgos.append(f"**En la foto '{f['name']}'**: {respuesta_parcial.content}")
                        except Exception as e:
                            hallazgos.append(f"**En la foto '{f['name']}'**: 🚨 Error de Groq ({str(e)}).")
                        
                        # PAUSA TÁCTICA PARA ENFRIAR GROQ (Solo si hay más fotos)
                        if i < len(pool_fotos[:4]) - 1:
                            st.info("⏳ Pausa de 12 segundos para no saturar la capa gratuita de Groq...")
                            time.sleep(12)
            
            # 3. MOSTRAR RESULTADO FINAL
            respuesta_final = "### 📋 Resumen de Hallazgos en Fotos:\n\n" + "\n\n".join(hallazgos)
            st.markdown(respuesta_final)
            st.session_state.messages.append({"role": "assistant", "content": respuesta_final})
