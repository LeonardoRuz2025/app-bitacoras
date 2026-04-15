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
st.set_page_config(page_title="Auditoría Técnica de Pozos", layout="wide")
st.title("📋 Gestión de Bitácoras Técnicas")

def get_drive_service():
    info_claves = json.loads(st.secrets["GOOGLE_JSON_COMPLETO"])
    creds = service_account.Credentials.from_service_account_info(info_claves)
    return build('drive', 'v3', credentials=creds)
    
def leer_archivo_multimodal(service, file_id, mime_type, file_name, fecha_mod):
    try:
        request = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        
        # Formatear fecha para la IA
        fecha_legible = fecha_mod.split('T')[0] # Extrae YYYY-MM-DD
        
        if 'image' in mime_type:
            img = Image.open(fh).convert('RGB')
            img.thumbnail((700, 700))
            buffered = BytesIO()
            img.save(buffered, format="JPEG", quality=75)
            encoded = base64.b64encode(buffered.getvalue()).decode('utf-8')
            return {"tipo": "imagen", "contenido": f"data:image/jpeg;base64,{encoded}", "nombre": file_name, "fecha": fecha_legible}
            
        elif mime_type == 'application/pdf':
            texto = " ".join([p.extract_text() for p in PdfReader(fh).pages[:15]])
            return {"tipo": "texto", "contenido": texto, "nombre": file_name, "fecha": fecha_legible}
        elif 'spreadsheet' in mime_type or 'csv' in mime_type or 'excel' in mime_type:
            df = pd.read_excel(fh) if 'spreadsheet' in mime_type else pd.read_csv(fh)
            return {"tipo": "texto", "contenido": df.head(100).to_string(), "nombre": file_name, "fecha": fecha_legible}
    except Exception: 
        return None
    return None

# --- INTERFAZ ---
if "messages" not in st.session_state: 
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]): 
        st.markdown(m["content"])

user_input = st.chat_input("Ej: ¿Cuándo fue el último registro del PBO-08?")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"): 
        st.markdown(user_input)

    with st.chat_message("assistant"):
        service = get_drive_service()
        
        # 1. BÚSQUEDA CRONOLÓGICA (Trae lo más nuevo primero)
        palabras_crudas = re.findall(r'[\w-]+', user_input)
        palabras_clave = [p for p in palabras_crudas if len(p) > 3 and p.lower() not in ['dame', 'fotos', 'serie', 'estan', 'carpeta', 'numeros', 'documentos', 'archivos', 'hizo', 'que', 'dia', 'labores', 'cuanto', 'ultima', 'registros']]
        if not palabras_clave: palabras_clave = [max(palabras_crudas, key=len)]
            
        pool_archivos = []
        seen_ids = set()
        
        with st.spinner("Buscando los registros más recientes en Drive..."):
            for t in palabras_clave[:2]:
                # Solicitamos orden descencente por fecha de modificación
                q_files = f"(name contains '{t}' or fullText contains '{t}') and trashed = false"
                files_out = service.files().list(q=q_files, fields="files(id, name, mimeType, modifiedTime)", orderBy="modifiedTime desc").execute().get('files', [])
                for f in files_out:
                    if f['id'] not in seen_ids:
                        pool_archivos.append(f)
                        seen_ids.add(f['id'])

        archivos_totales = pool_archivos[:35]
        
        if not archivos_totales:
            st.warning("No se encontró evidencia del pozo o término buscado.")
            st.stop()

        # 2. PROCESAMIENTO POR TANDAS
        resumenes_parciales = []
        tamanio_tanda = 7 
        
        # Usamos 1.5-flash por su estabilidad de cuota
        llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", google_api_key=st.secrets["GEMINI_API_KEY"])
        
        st.info(f"Se analizan los {len(archivos_totales)} registros más recientes encontrados.")
        progress_bar = st.progress(0)
        
        for n in range(0, len(archivos_totales), tamanio_tanda):
            tanda = archivos_totales[n:n+tamanio_tanda]
            contenidos_tanda = []
            
            for f in tanda:
                res = leer_archivo_multimodal(service, f['id'], f['mimeType'], f['name'], f['modifiedTime'])
                if res: contenidos_tanda.append(res)
            
            if contenidos_tanda:
                with st.spinner(f"Analizando bloque { (n // tamanio_tanda) + 1}..."):
                    prompt_tanda = f"""Extrae hitos y fechas. Si hay fotos o documentos, indica qué actividad se registra y la fecha del archivo. 
                    Contexto: {user_input}"""
                    
                    mensaje = [HumanMessage(content=[{"type": "text", "text": prompt_tanda}])]
                    for item in contenidos_tanda:
                        texto_item = f"\n\nARCHIVO: {item['nombre']} (Fecha: {item['fecha']})\nCONTENIDO: {item['contenido'] if item['tipo'] == 'texto' else 'Imagen adjunta'}"
                        mensaje[0].content[0]["text"] += texto_item
                        if item["tipo"] == "imagen":
                            mensaje[0].content.append({"type": "image_url", "image_url": {"url": item["contenido"]}})
                    
                    try:
                        resp_tanda = llm.invoke(mensaje)
                        resumenes_parciales.append(resp_tanda.content)
                    except Exception as e:
                        resumenes_parciales.append(f"Error en bloque: {str(e)}")
            
            progress_bar.progress(min((n + tamanio_tanda) / len(archivos_totales), 1.0))
            time.sleep(3) 

        # 3. CONSOLIDACIÓN FINAL (Corregida)
        with st.spinner("Determinando la última fecha de registro..."):
            texto_consolidado = "\n\n".join(resumenes_parciales)
            
            prompt_final = f"""
            Basándote en los hallazgos extraídos, responde la consulta del usuario.
            PREGUNTA: "{user_input}"
            
            HALLAZGOS CRONOLÓGICOS:
            {texto_consolidado}
            
            REGLAS:
            1. Reporte directo, técnico y SIN introducciones.
            2. Identifica claramente la fecha más reciente encontrada para el pozo consultado.
            3. Resume qué se hizo en esa última intervención.
            4. Menciona el nombre del archivo que respalda esa fecha.
            """
            
            try:
                # LLAMADA CORREGIDA: Usamos HumanMessage para evitar el error de consolidación
                respuesta_final = llm.invoke([HumanMessage(content=prompt_final)])
                st.markdown(respuesta_final.content)
                st.session_state.messages.append({"role": "assistant", "content": respuesta_final.content})
            except Exception as e:
                st.error(f"Error en el paso final: {str(e)}")
