import streamlit as st
from langchain_google_genai import ChatGoogleGenerativeAI
import os

# 1. Configuración de la página
st.set_page_config(page_title="Bitácoras IA", layout="centered")
st.title("🚜 Asistente de Terreno IA")
st.caption("Consulta las bitácoras y reportes de terreno guardados en Google Drive.")

# 2. Obtener la clave de Gemini desde los secretos (lo configuraremos en el Paso 4)
api_key = st.secrets.get("GEMINI_API_KEY", "CLAVE_NO_ENCONTRADA")

# 3. Inicializar el motor de IA
try:
    llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", google_api_key=api_key)
except Exception as e:
    st.error("Error al conectar con la IA. Revisa tu API Key.")

# 4. Memoria del chat
if "mensajes" not in st.session_state:
    st.session_state.mensajes = [{"role": "assistant", "content": "Hola. Soy tu asistente de terreno. ¿Qué necesitas consultar hoy?"}]

for msg in st.session_state.mensajes:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# 5. Interacción con el usuario
pregunta = st.chat_input("Ej: ¿Qué sensor se instaló el 13 de marzo?")

if pregunta:
    # Mostrar pregunta del usuario
    st.session_state.mensajes.append({"role": "user", "content": pregunta})
    with st.chat_message("user"):
        st.markdown(pregunta)
        
    # Generar respuesta de la IA
    with st.chat_message("assistant"):
        with st.spinner("Buscando en documentos..."):
            # NOTA PARA EL DESARROLLADOR: Aquí se inserta la lógica de descarga de Google Drive 
            # y búsqueda en ChromaDB usando el archivo credenciales.json.
            
            # Por ahora, le enviamos la pregunta directamente a Gemini como prueba de conexión:
            respuesta_ia = llm.invoke(pregunta).content
            st.markdown(respuesta_ia)
            
    # Guardar en memoria
    st.session_state.mensajes.append({"role": "assistant", "content": respuesta_ia})