import streamlit as st
import os
import pandas as pd
from io import BytesIO
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from langchain_groq import ChatGroq
from pypdf import PdfReader

# 1. Configuración inicial
st.set_page_config(page_title="Asistente de Bitácoras", layout="wide")
st.title("🚜 Sistema de Consultas de Terreno")

# 2. Conexión con las llaves (Secrets)
GROQ_API_KEY = st.secrets.get("GROQ_API_KEY")
CREDENTIALS_INFO = st.secrets.get("google_credentials") # Ahora lo guardaremos en Secrets por seguridad

# 3. Inicializar Servicios de Google Drive
def get_drive_service():
    creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    return build('drive', 'v3', credentials=creds)

# 4. Función para buscar y leer el contexto del Pozo en Drive
def buscar_contexto_pozo(nombre_pozo):
    service = get_drive_service()
    # Buscamos archivos o carpetas que contengan el nombre del pozo
    query = f"name contains '{nombre_pozo}'"
    results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    items = results.get('files', [])
    
    texto_extraido = ""
    for item in items:
        # Si es un PDF
        if item['mimeType'] == 'application/pdf':
            request = service.files().get_media(fileId=item['id'])
            fh = BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            reader = PdfReader(fh)
            for page in reader.pages:
                texto_extraido += page.extract_text()
        
        # Si es un Excel
        elif 'spreadsheet' in item['mimeType'] or 'csv' in item['mimeType']:
            request = service.files().get_media(fileId=item['id'])
            fh = BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            downloader.next_chunk()
            df = pd.read_excel(fh) if 'spreadsheet' in item['mimeType'] else pd.read_csv(fh)
            texto_extraido += df.to_string()
            
    return texto_extraido if texto_extraido else "No encontré archivos específicos para ese pozo."

# 5. Interfaz de Chat
if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

pregunta = st.chat_input("¿Qué quieres saber de los pozos?")

if pregunta:
    st.session_state.messages.append({"role": "user", "content": pregunta})
    with st.chat_message("user"):
        st.markdown(pregunta)

    # Lógica de Respuesta
    with st.chat_message("assistant"):
        with st.spinner("Consultando bitácoras en Drive..."):
            # Paso A: Extraer palabras clave (el nombre del pozo) de la pregunta
            # Por simplicidad, buscamos códigos que empiecen por PB o PM (ajustar según tus nombres)
            import re
            match = re.search(r'[P][B|M][\w-]+', pregunta.upper())
            pozo_detectado = match.group(0) if match else None
            
            contexto = ""
            if pozo_detectado:
                contexto = buscar_contexto_pozo(pozo_detectado)
            
            # Paso B: Enviar contexto a la IA (Groq)
            llm = ChatGroq(model="llama-3.1-8b-instant", groq_api_key=GROQ_API_KEY)
            
            prompt = f"""
            Eres un asistente técnico de terreno. 
            CONTEXTO DE DRIVE: {contexto}
            
            PREGUNTA DEL USUARIO: {pregunta}
            
            Responde de forma exacta basándote SOLO en el contexto de arriba. 
            Si no está la información, di que no aparece en los archivos de Drive.
            """
            
            respuesta = llm.invoke(prompt).content
            st.markdown(respuesta)
            st.session_state.messages.append({"role": "assistant", "content": respuesta})
