import streamlit as st
import re
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

# --- INICIO Y CONFIGURACIÓN ---
st.set_page_config(page_title="Asistente de Terreno Groq", layout="wide")
st.title("🚜 Asistente de Bitácoras (Motor Llama 3.2)")

GROQ_API_KEY = st.secrets.get("GROQ_API_KEY")

def get_drive_service():
    creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    return build('drive', 'v3', credentials=creds)

# --- FUNCIÓN PARA PROCESAR ARCHIVOS Y FOTOS ---
def procesar_archivo_groq(service, file_id, file_name, mime_type):
    try:
        request = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)

        # Si es FOTO (Convertir a Base64 para Groq)
        if "image" in mime_type:
            img = Image.open(fh)
            # NUEVO: Reducimos el tamaño de la foto para no saturar a Groq
            img.thumbnail((800, 800)) 
            buffered = BytesIO()
            img.save(buffered, format=img.format if img.format else "JPEG")
            
            encoded_string = base64.b64encode(buffered.getvalue()).decode('utf-8')
            formato = mime_type.split('/')[-1]
            if formato == "jpg": formato = "jpeg" # Groq es estricto con este nombre
            
            return {"tipo": "imagen", "contenido": f"data:image/{formato};base64,{encoded_string}"}
        
        # Si es PDF
        elif mime_type == 'application/pdf':
            reader = PdfReader(fh)
            texto = f"--- PDF: {file_name} ---\n"
            texto += " ".join([page.extract_text() for page in reader.pages])
            return {"tipo": "texto", "contenido": texto}

        # Si es EXCEL / CSV
        elif 'spreadsheet' in mime_type or 'csv' in mime_type or 'excel' in mime_type:
            df = pd.read_excel(fh) if 'spreadsheet' in mime_type else pd.read_csv(fh)
            return {"tipo": "texto", "contenido": f"--- Tabla: {file_name} ---\n{df.to_string()}"}
    except Exception as e:
        return None
    return None

# --- BUSCADOR RECURSIVO EN DRIVE ---
def buscar_recursivo(service, query_text):
    keywords = re.findall(r'\w+', query_text.lower())
    files_to_process = []
    
    for kw in keywords:
        if len(kw) < 2: continue
        q = f"name contains '{kw}' and trashed = false"
        results = service.files().list(q=q, fields="files(id, name, mimeType)").execute()
        for f in results.get('files', []):
            if f['mimeType'] == 'application/vnd.google-apps.folder':
                sub_res = service.files().list(q=f"'{f['id']}' in parents and trashed = false").execute()
                files_to_process.extend(sub_res.get('files', []))
            else:
                files_to_process.append(f)
    return files_to_process

# --- INTERFAZ DE CHAT ---
if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

pregunta = st.chat_input("Ej: ¿Qué se hizo en terreno el 6 de abril en Colana?")

if pregunta:
    st.session_state.messages.append({"role": "user", "content": pregunta})
    with st.chat_message("user"): st.markdown(pregunta)

    with st.chat_message("assistant"):
        with st.spinner("Buscando y leyendo documentos con Groq..."):
            service = get_drive_service()
            archivos = buscar_recursivo(service, pregunta)
            
            contexto_texto = ""
            imagenes_base64 = []
            
            # Limitar a 4 archivos para no saturar la memoria gratuita de Groq
            for f in archivos[:4]:
                res = procesar_archivo_groq(service, f['id'], f['name'], f['mimeType'])
                if res:
                    if res["tipo"] == "texto": contexto_texto += res["contenido"] + "\n"
                    if res["tipo"] == "imagen": imagenes_base64.append(res["contenido"])

            # PREPARAR EL MENSAJE PARA GROQ VISION
            # Inicializamos el modelo de Groq que soporta imágenes
            llm = ChatGroq(model="llama-3.2-11b-vision-preview", groq_api_key=GROQ_API_KEY)
            
            instruccion = f"""Eres un analista de bitácoras de terreno. 
            Responde la pregunta basándote estrictamente en este texto y en las fotos adjuntas.
            TEXTO EXTRAÍDO: {contexto_texto}
            PREGUNTA DEL USUARIO: {pregunta}
            Si no hay información suficiente en los archivos, dilo claramente."""

            # Construir el contenido multimodal (texto + imágenes)
            mensaje_contenido = [{"type": "text", "text": instruccion}]
            for img_url in imagenes_base64:
                mensaje_contenido.append({"type": "image_url", "image_url": {"url": img_url}})
            
            mensaje_final = HumanMessage(content=mensaje_contenido)

            # Invocar al modelo
            try:
                response = llm.invoke([mensaje_final])
                respuesta_texto = response.content
            except Exception as e:
                # NUEVO: Ahora imprimirá el error exacto que envía Groq
                respuesta_texto = f"🚨 Error técnico de Groq: {str(e)}"

            st.markdown(respuesta_texto)
            st.session_state.messages.append({"role": "assistant", "content": respuesta_texto})
