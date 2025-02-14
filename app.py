import streamlit as st
import pandas as pd
import os
import io
from pathlib import Path
from datetime import datetime
from analysis_script import run_analysis  # Importa sua função de análise

st.title("📊 Processador de Estoque Inteligente")

# Upload de múltiplos arquivos
uploaded_files = st.file_uploader("Escolha os arquivos Excel", type=["xlsx"], accept_multiple_files=True)

if uploaded_files:
    # Criar pasta temporária para salvar os arquivos
    temp_folder = Path(f"./data/{datetime.now().strftime('%Y-%m')}/")
    temp_folder.mkdir(parents=True, exist_ok=True)

    # Salvar os arquivos localmente
    for file in uploaded_files:
        file_path = temp_folder / file.name
        with open(file_path, "wb") as f:
            f.write(file.getbuffer())

    st.success("Arquivos carregados com sucesso!")

    # Botão para rodar a análise
    if st.button("🔍 Rodar Análise"):
        with st.spinner("Processando os dados..."):
            df_final = run_analysis(temp_folder)
        
        # Exibir uma amostra dos dados analisados
        st.write("📈 Dados Analisados:")
        st.dataframe(df_final.head())

        # Criar um arquivo Excel para download
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df_final.to_excel(writer, sheet_name="Resultados", index=False)
        output.seek(0)

        # Botão para baixar o arquivo processado
        st.download_button("📥 Baixar Planilha Analisada", output, file_name="resultado_analise.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
