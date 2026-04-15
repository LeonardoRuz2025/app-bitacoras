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
st.set_page_config(page_title="Sistema de Auditoría de Bitácoras", layout="wide")
st.title("📋 Análisis Técnico de Terreno")

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
            img.thumbnail((700, 700)) # Tamaño óptimo para tokens
            buffered = BytesIO()
            img.save(buffered, format="JPEG", quality=75)
            encoded = base64.b64encode(buffered.getvalue()).decode('utf-8')
            return {"tipo": "imagen", "contenido": f"data:image/jpeg;base64,{encoded}", "nombre": file_name}
            
        elif mime_type == 'application/pdf':
            reader = PdfReader(fh)
            # Leemos solo las páginas necesarias para no saturar (primeras 15)
            texto = " ".join([p.extract_text() for p in reader.pages[:15]])
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

user_input = st.chat_input("¿Qué labores se realizaron el día...?")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"): 
        st.markdown(user_input)

    with st.chat_message("assistant"):
        service = get_drive_service()
        
        # 1. BÚSQUEDA EN DRIVE
        palabras_crudas = re.findall(r'[\w-]+', user_input)
        # Filtramos palabras comunes para buscar solo lo técnico
        palabras_clave = [p for p in palabras_crudas if len(p) > 3 and p.lower() not in ['dame', 'fotos', 'serie', 'estan', 'carpeta', 'numeros', 'documentos', 'archivos', 'hizo', 'que', 'dia', 'labores', 'realizadas']]
        if not palabras_clave: palabras_clave = [max(palabras_crudas, key=len)]
            
        pool_archivos = []
        seen_ids = set()
        
        with st.spinner("Rastreando evidencias en Drive..."):
            for t in palabras_clave[:3]: # Limitamos términos para no saturar la búsqueda
                q_files = f"(name contains '{t}' or fullText contains '{t}') and trashed = false"
                files_out = service.files().list(q=q_files, fields="files(id, name, mimeType)").execute().get('files', [])
                for f in files_out:
                    if f['id'] not in seen_ids:
                        pool_archivos.append(f)
                        seen_ids.add(f['id'])

        # Capacidad para hasta 35 archivos
        archivos_totales = pool_archivos[:35]
        
        if not archivos_totales:
            st.warning("No se encontraron registros que coincidan con la búsqueda.")
            st.stop()

        # 2. PROCESAMIENTO POR TANDAS (BATCHES)
        resumenes_parciales = []
        tamanio_tanda = 7 # Analizamos de 7 en 7 para un balance perfecto entre velocidad y cuota
        
        # CONFIGURACIÓN DEL MODELO CON FALLBACK
        try:
            # Intentamos la versión estándar
            llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", google_api_key=st.secrets["GEMINI_API_KEY"])
        except:
            # Si falla, usamos la versión específica que suele arreglar el 404
            llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash-latest", google_api_key=st.secrets["GEMINI_API_KEY"])
        
        st.info(f"Analizando {len(archivos_totales)} evidencias encontradas...")
        progress_bar = st.progress(0)
        
        for n in range(0, len(archivos_totales), tamanio_tanda):
            tanda = archivos_totales[n:n+tamanio_tanda]
            contenidos_tanda = []
            
            for f in tanda:
                res = leer_archivo_multimodal(service, f['id'], f['mimeType'], f['name'])
                if res: contenidos_tanda.append(res)
            
            if contenidos_tanda:
                with st.spinner(f"Procesando bloque { (n // tamanio_tanda) + 1}..."):
                    prompt_tanda = f"""Extrae hitos, tareas realizadas y datos técnicos. 
                    Si hay fotos, describe qué se está haciendo o qué equipo se ve.
                    Contexto de búsqueda: {user_input}"""
                    
                    mensaje = [{"type": "text", "text": prompt_tanda}]
                    for item in contenidos_tanda:
                        if item["tipo"] == "texto":
                            mensaje[0]["text"] += f"\n\nARCHIVO: {item['nombre']}\nCONTENIDO: {item['contenido']}"
                        else:
                            mensaje.append({"type": "image_url", "image_url": {"url": item["contenido"]}})
                    
                    try:
                        resp_tanda = llm.invoke([HumanMessage(content=mensaje)])
                        resumenes_parciales.append(resp_tanda.content)
                    except Exception as e:
                        resumenes_parciales.append(f"Bloque omitido por error técnico.")
            
            progress_bar.progress(min((n + tamanio_tanda) / len(archivos_totales), 1.0))
            time.sleep(2) # Pausa breve para no saturar el tráfico

        # 3. CONSOLIDACIÓN FINAL
        with st.spinner("Consolidando reporte técnico..."):
            texto_consolidado = "\n\n".join(resumenes_parciales)
            
            prompt_final = f"""
            Genera un reporte técnico basado exclusivamente en los hallazgos de terreno extraídos.
            PREGUNTA: "{user_input}"
            
            HALLAZGOS:
            {texto_consolidado}
            
            REGLAS:
            1. Prohibido usar introducciones como "Como Ingeniero experto" o saludos. Ve directo a los datos.
            2. Si la consulta pide labores de un día, considera cada hallazgo como un evento realizado.
            3. Organiza la respuesta de forma estructurada (por pozo o por actividad).
            4. Menciona el nombre del archivo fuente para cada punto clave.
            5. Si no hay datos suficientes, responde de forma breve que no se encontró evidencia específica.
            """
            
            try:
                # Usamos el mismo LLM para consolidar
                respuesta_final = llm.invoke(prompt_final)
                st.markdown(respuesta_final.content)
                st.session_state.messages.append({"role": "assistant", "content": respuesta_final.content})
            except Exception as e:
                st.error("Error al consolidar el reporte. Por favor, intente con una búsqueda más acotada.")
