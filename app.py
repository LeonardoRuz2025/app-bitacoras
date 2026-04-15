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
st.set_page_config(page_title="Analista Masivo (Gemini)", layout="wide")
st.title("🚀 Analista de Terreno Masivo (Motor Gemini 1.5)")

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
            # Ya no comprimimos tanto la foto porque Gemini tiene memoria gigante
            img = Image.open(fh).convert('RGB')
            img.thumbnail((1024, 1024)) 
            buffered = BytesIO()
            img.save(buffered, format="JPEG", quality=90)
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

user_input = st.chat_input("Pregunta sobre pozos, fotos o documentos...")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"): 
        st.markdown(user_input)

    with st.chat_message("assistant"):
        service = get_drive_service()
        
        # 1. BÚSQUEDA INTELIGENTE EN DRIVE
        palabras_crudas = re.findall(r'[\w-]+', user_input)
        palabras_clave = [p for p in palabras_crudas if len(p) > 3 and p.lower() not in ['dame', 'fotos', 'serie', 'estan', 'carpeta', 'numeros', 'documentos', 'archivos']]
        if not palabras_clave: 
            palabras_clave = [max(palabras_crudas, key=len)]
            
        pool_archivos = []
        seen_ids = set()
        
        with st.spinner("Rastreando la carpeta y sus archivos en Drive..."):
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

        if not pool_archivos:
            st.warning("No encontré información en tu Drive relacionada con esta búsqueda.")
            st.stop()

        # 2. DESCARGA Y PREPARACIÓN MASIVA
        textos_extraidos = ""
        imagenes_base64 = []
        
        st.success(f"¡Se localizaron {len(pool_archivos)} archivos! Procesando todo en un solo bloque...")
        
        # Leemos TODOS los archivos sin limitarnos a 3 o 4
        bar = st.progress(0)
        for i, f in enumerate(pool_archivos):
            res = leer_archivo_multimodal(service, f['id'], f['mimeType'], f['name'])
            if res:
                if res["tipo"] == "texto":
                    textos_extraidos += f"\n--- Archivo: {f['name']} ---\n{res['contenido']}\n"
                elif res["tipo"] == "imagen":
                    imagenes_base64.append({"url": res["contenido"], "nombre": f['name']})
            bar.progress((i + 1) / len(pool_archivos))
            
        # 3. ENVÍO MASIVO A GEMINI 1.5 FLASH
        with st.spinner("🧠 Gemini está analizando todos los textos y fotos simultáneamente..."):
            llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", google_api_key=st.secrets["GEMINI_API_KEY"])
            
            prompt_maestro = f"""
            Eres un Ingeniero experto en análisis de datos de terreno. 
            A continuación se te entregan documentos de texto y fotografías extraídas directamente de las carpetas de operaciones.
            
            PREGUNTA DEL USUARIO: "{user_input}"
            
            TEXTOS ENCONTRADOS:
            {textos_extraidos}
            
            INSTRUCCIONES CRÍTICAS:
            1. Analiza TODA la información (textos y fotos adjuntas).
            2. Si el usuario pregunta por un pozo específico (Ej: PBPC-06), ignora cualquier foto o documento que claramente pertenezca a otro pozo (Ej: PBPC-01).
            3. Responde a la pregunta de manera estructurada, indicando exactamente de qué archivo o foto sacaste el dato.
            4. Si la información no aparece ni en los textos ni en las fotos correctas, dilo claramente.
            """
            
            # Construir el paquete masivo
            mensaje_contenido = [{"type": "text", "text": prompt_maestro}]
            for img in imagenes_base64:
                mensaje_contenido.append({"type": "image_url", "image_url": {"url": img["url"]}})
                
            try:
                # Enviamos todo de un solo golpe. 
                response = llm.invoke([HumanMessage(content=mensaje_contenido)])
                respuesta_final = response.content
            except Exception as e:
                respuesta_final = f"🚨 Ocurrió un error al consultar a Gemini: {str(e)}"
                
        st.markdown(respuesta_final)
        st.session_state.messages.append({"role": "assistant", "content": respuesta_final})
