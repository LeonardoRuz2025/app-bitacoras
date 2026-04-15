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

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Analista Multimodal de Terreno", layout="wide")
st.title("👁️🧠 Inteligencia de Terreno Avanzada (Textos y Fotos)")

def get_drive_service():
    creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    return build('drive', 'v3', credentials=creds)

# --- FUNCIÓN LECTORA DE TEXTOS Y FOTOS ---
def leer_archivo_multimodal(service, file_id, mime_type, file_name):
    try:
        request = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        
        # SI ES FOTO (Aplicamos compresión extrema para no crashear Groq)
        if 'image' in mime_type:
            img = Image.open(fh).convert('RGB')
            img.thumbnail((600, 600)) # Reducimos resolución drásticamente
            buffered = BytesIO()
            img.save(buffered, format="JPEG", quality=70) # Reducimos calidad para que pese menos tokens
            encoded = base64.b64encode(buffered.getvalue()).decode('utf-8')
            return {"tipo": "imagen", "contenido": f"data:image/jpeg;base64,{encoded}"}
            
        # SI ES TEXTO
        elif mime_type == 'application/pdf':
            return {"tipo": "texto", "contenido": " ".join([p.extract_text() for p in PdfReader(fh).pages])}
        elif 'spreadsheet' in mime_type or 'csv' in mime_type:
            df = pd.read_excel(fh) if 'spreadsheet' in mime_type else pd.read_csv(fh)
            return {"tipo": "texto", "contenido": df.to_string()}
    except Exception as e:
        return None
    return None

# --- BÚSQUEDA PROFUNDA EN DRIVE ---
def buscar_y_procesar(service, query_text):
    # Extraer palabras manteniendo guiones (ej: PBPC-06)
    palabras_crudas = re.findall(r'[\w-]+', query_text)
    # Filtramos palabras de relleno
    palabras_clave = [p for p in palabras_crudas if len(p) > 3 and p.lower() not in ['dame', 'fotos', 'los', 'del', 'las', 'numeros', 'serie']]
    
    if not palabras_clave: palabras_clave = [max(palabras_crudas, key=len)]
        
    textos = ""
    imagenes_base64 = []
    archivos_procesados = 0
    
    for t in palabras_clave:
        # Buscamos archivos que contengan el término en su texto (incluso OCR automático de Drive en fotos) o nombre
        q = f"fullText contains '{t}' and trashed = false"
        results = service.files().list(q=q, fields="files(id, name, mimeType)").execute()
        
        # Tomamos máximo 3 archivos para no superar el límite de 12.000 tokens de Groq
        for f in results.get('files', [])[:3]: 
            if archivos_procesados >= 3: break
            
            icono = "🖼️" if "image" in f['mimeType'] else "📄"
            st.write(f"{icono} Procesando: {f['name']}...")
            
            res = leer_archivo_multimodal(service, f['id'], f['mimeType'], f['name'])
            if res:
                if res["tipo"] == "texto":
                    textos += f"\n--- {f['name']} ---\n{res['contenido']}\n"
                elif res["tipo"] == "imagen":
                    imagenes_base64.append(res["contenido"])
                archivos_procesados += 1
                
    # Cortafuegos para el texto (dejamos espacio para que las fotos quepan en el envío)
    if len(textos) > 8000: textos = textos[:8000] + "\n...[RECORTADO]"
    return textos, imagenes_base64

# --- INTERFAZ ---
if "messages" not in st.session_state: st.session_state.messages = []
for m in st.session_state.messages:
    with st.chat_message(m["role"]): st.markdown(m["content"])

user_input = st.chat_input("Escribe tu pregunta (Ej: Dime los números de serie del PBPC-06 en las fotos)...")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"): st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Buscando documentos y analizando imágenes..."):
            service = get_drive_service()
            textos, imagenes = buscar_y_procesar(service, user_input)
            
            # Inicializamos el nuevo modelo de visión (Llama 4 Scout)
            llm = ChatGroq(model="meta-llama/llama-4-scout-17b-16e-instruct", groq_api_key=st.secrets["GROQ_API_KEY"])
            
            prompt_texto = f"""
            Eres un experto analizando bitácoras de terreno y equipamiento de pozos.
            TEXTO EXTRAÍDO: {textos}
            PREGUNTA: {user_input}
            
            INSTRUCCIONES:
            1. Analiza el texto extraído.
            2. Analiza detenidamente todas las FOTOS adjuntas en este mensaje. Busca placas, seriales escritos a mano o etiquetas.
            3. Responde de manera técnica, mencionando de qué documento o foto sacaste el dato.
            """
            
            # Construimos el "paquete" con el texto y las fotos
            mensaje_contenido = [{"type": "text", "text": prompt_texto}]
            for img in imagenes:
                mensaje_contenido.append({"type": "image_url", "image_url": {"url": img}})
                
            try:
                response = llm.invoke([HumanMessage(content=mensaje_contenido)])
                respuesta_final = response.content
            except Exception as e:
                respuesta_final = f"🚨 Ocurrió un error en el servidor de Groq. Esto puede deberse al tamaño de las fotos: {str(e)}"
                
            st.markdown(respuesta_final)
            st.session_state.messages.append({"role": "assistant", "content": respuesta_final})
