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
st.title("🚜 Analista Multimodal Completo (Textos y Fotos)")

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
        
        # Procesar Fotos
        if 'image' in mime_type:
            img = Image.open(fh).convert('RGB')
            img.thumbnail((800, 800)) 
            buffered = BytesIO()
            img.save(buffered, format="JPEG", quality=80)
            encoded = base64.b64encode(buffered.getvalue()).decode('utf-8')
            return {"tipo": "imagen", "contenido": f"data:image/jpeg;base64,{encoded}"}
            
        # Procesar Textos (PDF y Tablas)
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

user_input = st.chat_input("Ej: Dame los números de serie del PBPC-06 en las fotos y documentos")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"): 
        st.markdown(user_input)

    with st.chat_message("assistant"):
        service = get_drive_service()
        
        # 1. BÚSQUEDA INTELIGENTE
        palabras_crudas = re.findall(r'[\w-]+', user_input)
        # Filtramos palabras basura para no saturar la búsqueda
        palabras_clave = [p for p in palabras_crudas if len(p) > 3 and p.lower() not in ['dame', 'fotos', 'serie', 'estan', 'carpeta', 'numeros', 'documentos', 'archivos']]
        if not palabras_clave: 
            palabras_clave = [max(palabras_crudas, key=len)]
            
        pool_archivos = []
        seen_ids = set()
        
        with st.spinner("Rastreando carpetas y archivos en Drive..."):
            for t in palabras_clave:
                # Paso A: Buscar carpetas
                q_folder = f"name contains '{t}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
                folders = service.files().list(q=q_folder).execute().get('files', [])
                
                for folder in folders:
                    q_in_folder = f"'{folder['id']}' in parents and trashed = false"
                    files_in = service.files().list(q_in_folder).execute().get('files', [])
                    for f in files_in:
                        if f['id'] not in seen_ids:
                            pool_archivos.append(f)
                            seen_ids.add(f['id'])
                
                # Paso B: Buscar archivos sueltos por nombre o texto
                q_files = f"(name contains '{t}' or fullText contains '{t}') and trashed = false"
                files_out = service.files().list(q=q_files).execute().get('files', [])
                for f in files_out:
                    if f['id'] not in seen_ids:
                        pool_archivos.append(f)
                        seen_ids.add(f['id'])

        # Separar en dos grupos: Fotos y Documentos
        pool_fotos = [f for f in pool_archivos if 'image' in f['mimeType']]
        pool_documentos = [f for f in pool_archivos if 'image' not in f['mimeType']]
        
        if not pool_fotos and not pool_documentos:
            st.warning("No encontré información en esa carpeta o con ese nombre.")
            st.stop()

        llm = ChatGroq(model="meta-llama/llama-4-scout-17b-16e-instruct", groq_api_key=st.secrets["GROQ_API_KEY"])
        hallazgos_texto = []
        hallazgos_fotos = []
        
        st.success(f"¡Localizados {len(pool_documentos)} documentos y {len(pool_fotos)} fotos! Iniciando análisis dual...")

        # --- 2. FASE A: ANÁLISIS DE DOCUMENTOS (PDF/Excel) ---
        if pool_documentos:
            textos_extraidos = ""
            # Limitamos a 3 documentos para no pasar el límite de 12k tokens de Groq
            docs_a_leer = pool_documentos[:3] 
            
            with st.spinner("📄 Analizando documentos de texto y tablas..."):
                for f in docs_a_leer:
                    res = leer_archivo_multimodal(service, f['id'], f['mimeType'], f['name'])
                    if res and res["tipo"] == "texto":
                        textos_extraidos += f"\n--- Archivo: {f['name']} ---\n{res['contenido']}\n"
            
            if textos_extraidos:
                if len(textos_extraidos) > 15000:
                    textos_extraidos = textos_extraidos[:15000] + "\n...[RECORTADO]"
                    
                prompt_docs = f"""
                Eres un experto técnico. Lee los siguientes documentos extraídos de Drive y responde la pregunta.
                PREGUNTA DEL USUARIO: "{user_input}"
                
                TEXTOS EXTRAÍDOS:
                {textos_extraidos}
                
                INSTRUCCIONES:
                1. Busca datos específicos que respondan la pregunta (ej. números de serie, fechas, etc.).
                2. Menciona el nombre del archivo de donde sacaste la información.
                3. Si los documentos no contienen la respuesta, responde EXACTAMENTE: "Sin datos relevantes en los documentos."
                """
                try:
                    resp_docs = llm.invoke(prompt_docs)
                    if "Sin datos relevantes" not in resp_docs.content:
                        hallazgos_texto.append(resp_docs.content)
                except Exception as e:
                    hallazgos_texto.append("🚨 Error al procesar los documentos por límite de tamaño de Groq.")

        # --- 3. FASE B: ANÁLISIS DE FOTOS (SECUENCIAL Y FILTRADO) ---
        if pool_fotos:
            total_fotos = len(pool_fotos)
            for i, f in enumerate(pool_fotos):
                progress_text = f"🖼️ Analizando foto {i+1} de {total_fotos}: {f['name']}"
                with st.spinner(progress_text):
                    res = leer_archivo_multimodal(service, f['id'], f['mimeType'], f['name'])
                    
                    if res and res["tipo"] == "imagen":
                        prompt_foto = f"""
                        Analiza esta fotografía de terreno.
                        PREGUNTA DEL USUARIO: "{user_input}"
                        
                        INSTRUCCIONES DE FILTRADO ESTRICTO (¡CRÍTICO!):
                        1. Identifica de qué pozo u objetivo trata la pregunta (Ej: PBPC-06).
                        2. Mira atentamente la foto. Si ves un letrero, papel o pizarra que indique que la foto pertenece a un pozo DIFERENTE (ej. PBPC-01, PBPC-05), DETENTE y responde EXACTAMENTE: "DESCARTADA".
                        3. Si la foto es del pozo correcto, extrae la información solicitada.
                        4. Si es correcta pero no ves la información (ej. no hay seriales visibles), responde EXACTAMENTE: "SIN DATOS".
                        """
                        
                        try:
                            resp = llm.invoke([HumanMessage(content=[
                                {"type": "text", "text": prompt_foto},
                                {"type": "image_url", "image_url": {"url": res["contenido"]}}
                            ])])
                            
                            respuesta_ia = resp.content.strip()
                            
                            if "DESCARTADA" in respuesta_ia.upper():
                                pass # Ignorar foto de otro pozo
                            elif "SIN DATOS" in respuesta_ia.upper():
                                pass # Ignorar foto sin información útil
                            else:
                                hallazgos_fotos.append(f"✅ **{f['name']}**: {respuesta_ia}")
                        except Exception as e:
                            hallazgos_fotos.append(f"❌ **{f['name']}**: Error al procesar imagen.")
                        
                        # Pausa de enfriamiento para Groq
                        if i < total_fotos - 1:
                            time.sleep(12) 
        
        # --- 4. RENDERIZAR RESULTADO UNIFICADO ---
        respuesta_final = ""
        
        if hallazgos_texto:
            respuesta_final += "### 📄 Información en Documentos (PDF/Excel):\n"
            for h in hallazgos_texto: 
                respuesta_final += f"{h}\n\n"
                
        if hallazgos_fotos:
            respuesta_final += "### 🖼️ Información en Fotografías:\n"
            for h in hallazgos_fotos: 
                respuesta_final += f"{h}\n\n"
        elif pool_fotos and not hallazgos_fotos:
            respuesta_final += "### 🖼️ Fotografías:\nRevisé las fotos pero no encontré los datos solicitados, o las fotos pertenecían a otros pozos.\n\n"

        if not respuesta_final:
            respuesta_final = "No se encontraron datos relevantes ni en los documentos ni en las fotos para esa consulta."

        st.markdown(respuesta_final)
        st.session_state.messages.append({"role": "assistant", "content": respuesta_final})
