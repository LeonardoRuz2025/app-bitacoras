import json
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
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
from pypdf import PdfReader

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Reporte Técnico de Terreno", layout="wide")
st.title("📋 Gestión de Bitácoras Técnicas")

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
            img.thumbnail((700, 700)) 
            buffered = BytesIO()
            img.save(buffered, format="JPEG", quality=75)
            encoded = base64.b64encode(buffered.getvalue()).decode('utf-8')
            return {"tipo": "imagen", "contenido": f"data:image/jpeg;base64,{encoded}", "nombre": file_name}
            
        elif mime_type == 'application/pdf':
            texto = " ".join([p.extract_text() for p in PdfReader(fh).pages[:15]])
            return {"tipo": "texto", "contenido": texto, "nombre": file_name}
        elif 'spreadsheet' in mime_type or 'csv' in mime_type or 'excel' in mime_type:
            df = pd.read_excel(fh) if 'spreadsheet' in mime_type else pd.read_csv(fh)
            return {"tipo": "texto", "contenido": df.head(100).to_string(), "nombre": file_name}
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
        
        # 1. BÚSQUEDA INTELIGENTE AMPLIADA
        palabras_crudas = re.findall(r'[\w-]+', user_input)
        palabras_clave = [p for p in palabras_crudas if len(p) > 3 and p.lower() not in ['dame', 'fotos', 'serie', 'estan', 'carpeta', 'numeros', 'documentos', 'archivos']]
        if not palabras_clave: palabras_clave = [max(palabras_crudas, key=len)]
            
        pool_archivos = []
        seen_ids = set()
        
        with st.spinner("Rastreando registros en Drive..."):
            for t in palabras_clave:
                q_files = f"(name contains '{t}' or fullText contains '{t}') and trashed = false"
                files_out = service.files().list(q=q_files, fields="files(id, name, mimeType)").execute().get('files', [])
                for f in files_out:
                    if f['id'] not in seen_ids:
                        pool_archivos.append(f)
                        seen_ids.add(f['id'])

        # Capacidad ampliada a 35 archivos
        archivos_totales = pool_archivos[:35]
        
        if not archivos_totales:
            st.warning("No se encontró información que coincida con la búsqueda.")
            st.stop()

        # 2. PROCESAMIENTO POR TANDAS (BATCHES)
        resumenes_parciales = []
        tamanio_tanda = 5 # Analizamos de 5 en 5 para no saturar la API
        
        llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", google_api_key=st.secrets["GEMINI_API_KEY"])
        
        st.info(f"Se han localizado {len(archivos_totales)} archivos. Iniciando análisis por tandas...")
        progress_bar = st.progress(0)
        
        for n in range(0, len(archivos_totales), tamanio_tanda):
            tanda = archivos_totales[n:n+tamanio_tanda]
            contenidos_tanda = []
            
            # Descarga de la tanda actual
            for i, f in enumerate(tanda):
                res = leer_archivo_multimodal(service, f['id'], f['mimeType'], f['name'])
                if res: contenidos_tanda.append(res)
            
            # Análisis de la tanda actual
            if contenidos_tanda:
                with st.spinner(f"Analizando tanda { (n // tamanio_tanda) + 1}..."):
                    prompt_tanda = f"Extrae de forma directa y técnica todos los eventos, labores, mediciones o cambios de sensores mencionados en estos archivos. Si es una foto, describe lo que se ve técnicamente. Pregunta del usuario: {user_input}"
                    
                    mensaje = [{"type": "text", "text": prompt_tanda}]
                    for item in contenidos_tanda:
                        if item["tipo"] == "texto":
                            mensaje[0]["text"] += f"\n\nARCHIVO {item['nombre']}:\n{item['contenido']}"
                        else:
                            mensaje.append({"type": "image_url", "image_url": {"url": item["contenido"]}})
                    
                    try:
                        resp_tanda = llm.invoke([HumanMessage(content=mensaje)])
                        resumenes_parciales.append(resp_tanda.content)
                    except Exception as e:
                        resumenes_parciales.append(f"Error en tanda: {str(e)}")
            
            # Actualizar barra y esperar un poco para no quemar la cuota
            progress_bar.progress(min((n + tamanio_tanda) / len(archivos_totales), 1.0))
            time.sleep(4) 

        # 3. CONSOLIDACIÓN FINAL
        with st.spinner("Redactando reporte final consolidado..."):
            texto_consolidado = "\n\n".join(resumenes_parciales)
            
            prompt_final = f"""
            Genera un reporte técnico final basado en los siguientes hallazgos parciales de terreno.
            CONSULTA DEL USUARIO: "{user_input}"
            
            HALLAZGOS EXTRAÍDOS:
            {texto_consolidado}
            
            REGLAS:
            1. Reporte DIRECTO, técnico y sin introducciones.
            2. Agrupa la información por hitos, actividades o pozos.
            3. Si la consulta pide "labores", describe cronológicamente lo que los archivos demuestran que se hizo.
            4. Menciona los nombres de los archivos citados en los hallazgos.
            5. Si hay contradicciones entre un plan y una bitácora, prioriza la bitácora.
            """
            
            try:
                respuesta_final = llm.invoke(prompt_final)
                st.markdown(respuesta_final.content)
                st.session_state.messages.append({"role": "assistant", "content": respuesta_final.content})
            except Exception as e:
                st.error(f"Error en consolidación: {str(e)}")
