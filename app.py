import streamlit as st
import re
import pandas as pd
import base64
from io import BytesIO
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage
from pypdf import PdfReader

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Analista Experto de Bitácoras", layout="wide")
st.title("🧠 Inteligencia de Terreno Avanzada")

def get_drive_service():
    creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    return build('drive', 'v3', credentials=creds)

# --- FUNCIÓN DE LECTURA DE CONTENIDO ---
def leer_archivo(service, file_id, mime_type):
    try:
        request = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        
        if mime_type == 'application/pdf':
            return " ".join([p.extract_text() for p in PdfReader(fh).pages])
        elif 'spreadsheet' in mime_type or 'csv' in mime_type:
            df = pd.read_excel(fh) if 'spreadsheet' in mime_type else pd.read_csv(fh)
            return df.to_string()
        return ""
    except: return ""

# --- BÚSQUEDA TIPO "GEMINI" (FULL TEXT) ---
def buscar_inteligente(service, query_text):
    # Paso 1: Buscar por CONTENIDO de texto, no solo nombre
    # Limpiamos la query para buscar términos clave
    terminos = re.findall(r'\w+', query_text)
    contexto = ""
    
    # Buscamos archivos que contengan las palabras clave en su interior
    for t in terminos:
        if len(t) < 3: continue
        # Esta es la clave: buscamos en el contenido completo
        q = f"fullText contains '{t}' and trashed = false"
        results = service.files().list(q=q, fields="files(id, name, mimeType)").execute()
        
        for f in results.get('files', [])[:5]: # Tomamos los 5 más relevantes por término
            st.write(f"🔍 Analizando: {f['name']}...")
            contenido = leer_archivo(service, f['id'], f['mimeType'])
            if contenido:
                contexto += f"\n--- ORIGEN: {f['name']} ---\n{contenido}\n"
    
    return contexto

# --- CHAT ---
if "messages" not in st.session_state: st.session_state.messages = []
for m in st.session_state.messages:
    with st.chat_message(m["role"]): st.markdown(m["content"])

user_input = st.chat_input("Escribe tu consulta técnica aquí...")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"): st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Realizando búsqueda profunda en Drive..."):
            service = get_drive_service()
            contexto_encontrado = buscar_inteligente(service, user_input)
            
            # Usamos el modelo más potente de Groq disponible (Llama 3.3 70B)
            # Es mucho más capaz de entender fechas y contextos que el Scout o el 8B
            llm = ChatGroq(model="llama-3.3-70b-versatile", groq_api_key=st.secrets["GROQ_API_KEY"])
            
            prompt = f"""
            Eres un Ingeniero Senior experto en análisis de bitácoras mineras y de pozos.
            
            TU BASE DE DATOS REAL (Extraída de Drive):
            {contexto_encontrado}
            
            INSTRUCCIÓN CRÍTICA:
            1. Analiza las fechas con cuidado. Si el usuario pregunta por el '9 de abril', busca cualquier registro cercano o que mencione esa labor.
            2. Si se menciona un pozo como 'PBPC-06', busca en tablas y textos técnicos.
            3. Responde de forma profesional y detallada. 
            4. Si el contexto está vacío, explica que el buscador de Drive no devolvió archivos con esos términos.
            
            PREGUNTA: {user_input}
            """
            
            response = llm.invoke(prompt)
            st.markdown(response.content)
            st.session_state.messages.append({"role": "assistant", "content": response.content})
