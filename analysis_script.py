import os
import shutil
import time
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, List
import xlsxwriter

import numpy as np
import pandas as pd

# --- Inicialização dos clientes de IA ---
# Substitua as chaves de API pelos seus valores reais.
from openai import OpenAI
import anthropic
#import google.generativeai as genai

openai_client = OpenAI(api_key="YOUR_OPENAI_API_KEY")
anthropic_client = anthropic.Anthropic(api_key="YOUR_ANTHROPIC_API_KEY")
deepseek_client = OpenAI(api_key="YOUR_DEEPSEEK_API_KEY", base_url="https://api.deepseek.com")
#genai.configure(api_key="YOUR_GENAI_API_KEY")
#model = genai.GenerativeModel('gemini-2.0-flash-001')

perplexity_api_key = "YOUR_PERPLEXITY_API_KEY"

# --- Configurações de análise ---
CV_THRESHOLD = 0.8           # Threshold do coeficiente de variação
TMD_THRESHOLD = 1.4          # Threshold do tempo médio entre demandas
ORDER_ZE_TRESHOLD = 500      # Threshold de valor para a política ZE
GORDURA_PR = 0               # Buffer percentual para ponto de reposição
GORDURA_MAX = 0              # Buffer percentual para estoque máximo
BAIXO_VALOR = 100            # Valor baixo de pedido
ALTO_VOLUME = 10000000       # Volume alto em CM³
ANOS_SEM_OC = 3              # Anos sem pedido
DEMAND_WINDOW = 3            # Janela de análise (anos)
LT_WINDOW = 12               # Janela de análise de lead time (meses)
k = 6                        # Constante para EOQ (não utilizada neste exemplo)
DEBUG = True
PERIODO_ANALISE_TEXTO = 90
PERIODO_PESQUISA = 90

# --- Função para carregar os dados ---
def load_data(data_folder: Path):
    op = pd.read_excel(data_folder / 'OP.XLSX', 
                      usecols=["Data abertura plan.","Material","Txt.brv.material",
                              "Grupo de mercadorias","Setor de atividade","Nº peça fabricante",
                              "Planejador MRP","Grupo MRP","Tipo de MRP","Prz.entrg.prev.",
                              "Estoque total","Ponto reabastec.","Estoque máximo",
                              "Estoque de segurança","Valor total","CMM","Demanda",
                              "Demanda Med.","Preço Unit.","Qtd. RTP1","Qtd. RTP2",
                              "Qtd. RTP3","Qtd. RTP6","Sld. Virtual","Qtd.ordem planejada",
                              "Valor Total","Responsável","Criticidade","Qtd. LMR",
                              "Dem. Pro.","Dt. Ult. Pedido","Fornecedor","Nome",
                              "Dt. Ult. Requisição","Qtd. Pedido","Qtd. Requisição",
                              "Qtd. RemCG","Dt. Ult. 201","Qt. 201 - 12 Meses"])
    
    t0053 = pd.read_excel(data_folder / '0053.XLSX',
                         thousands='.', decimal=',', 
                         usecols=['Material','Volume'])
    
    t0028 = pd.read_excel(data_folder / '0028.XLSX',
                         usecols=['Material','Tipo de reserva','Centro custo',
                                  'Data base','Nome do usuário','Cód. Localização',
                                  'Descrição do Equipamento','Material','Texto',
                                  'Com registro final','Item foi eliminado',
                                  'Motivo da Reserva','Qtd.retirada'])
    
    t0127 = pd.read_excel(data_folder / '0127.XLSX',
                         usecols=['Material','Texto OBS - pt','Texto OBS - es',
                                  'Texto DB - pt','Texto DB - es','Texto - pt',
                                  'Texto - es','Texto REF LMR'])
    
    t0130 = pd.read_excel(data_folder / '0130.XLSX',
                         nrows=10000, thousands='.', decimal=',',
                         usecols=['Material'] + [f'{i} LTD' for i in range(1, 16)])
    
    return op, t0130, t0053, t0028, t0127

# --- Função principal de análise ---
def run_analysis(data_folder: Path, config: dict = None) -> pd.DataFrame:
    """
    Executa a análise completa de estoque.
    
    :param data_folder: Caminho para a pasta de dados (por exemplo, ./data/YYYY-MM/)
    :param config: Dicionário de configurações (opcional)
    :return: DataFrame final com os resultados da análise.
    """
    if config is None:
        config = {}
    data_folder = Path(data_folder)
    os.makedirs(data_folder, exist_ok=True)
    print(f'Extracting data for {data_folder.name}...')
    
    # Verificar arquivos necessários
    required_files = ['OP.XLSX', '0130.XLSX', '0053.XLSX', '0028.XLSX', '0127.XLSX']
    for file in required_files:
        if not (data_folder / file).exists():
            print(f'File {file} not found in {data_folder}')
    
    # Carregar dados
    op, t0130, t0053, t0028, t0127 = load_data(data_folder)
    
    # Pré-processamento
    STRING_COLUMNS = [
        'Grupo de mercadorias', 'Setor de atividade', 'Nº peça fabricante',
        'Planejador MRP', 'Grupo MRP', 'Tipo de MRP', 'Responsável',
        'Fornecedor', 'Nome'
    ]
    DATE_COLUMNS = ['Data abertura plan.', 'Dt. Ult. Pedido', 'Dt. Ult. 201', 'Dt. Ult. Requisição']
    LT_COLUMNS = [f'{i} LTD' for i in range(1, 16)]
    
    for df in [op, t0130, t0053, t0028, t0127]:
        df['Material'] = df['Material'].astype(str)
    t0053['Material'] = t0053['Material'].str.replace('.0', '', regex=False)
    
    # Filtrar reservas pela janela de análise
    t0028['Data base'] = pd.to_datetime(t0028['Data base'], format='%d.%m.%Y')
    t0028 = t0028[t0028['Data base'] > datetime.now() - pd.DateOffset(years=DEMAND_WINDOW)]
    
    # Merge de dados
    df = op.copy()
    df = df.merge(t0130, on='Material', how='left')
    df = df.merge(t0053, on='Material', how='left')
    
    for col in STRING_COLUMNS:
        df[col] = df[col].astype(str)
    for col in DATE_COLUMNS:
        df[col] = pd.to_datetime(df[col], format='%d.%m.%Y', errors='coerce')
    for col in LT_COLUMNS:
        df[col] = df[col].replace('-', '')
        df[col] = np.ceil(df[col])
    
    df['Volume da OP'] = (df['Volume'] * df['Qtd.ordem planejada']).fillna(0).astype(int)
    analysis_columns = {
        'pre-analise': '',
        'Image': '',
        'PR sug': '',
        'MAX sug': '',
        'impacto $ da sugestao': "",
        'tendencia': '',
        'outliers': '',
        'Classificação': '',
        'Ajustes': 'sem ajuste',
        'analise': 'sem analise',
        'url': '-',
        'preco_mercado': '-',
        'matches_description': '-',
        'disponivel': '-',
        'comentarios': 'Sem comentarios'
    }
    for col, default_value in analysis_columns.items():
        df[col] = default_value
    
    # --- Funções auxiliares internas ---
    def TMD(row):
        consumo = row[LT_COLUMNS][:LT_WINDOW]
        non_zero = consumo[consumo != 0].count()
        total = consumo.count()
        return total/non_zero if (non_zero and total > 1) else -1

    def CV(row):
        consumo = row[LT_COLUMNS][:LT_WINDOW]
        std = np.std(consumo, ddof=0)
        mean = consumo.mean()
        return std/mean if (mean and consumo.count() > 1) else -1

    def classify(row):
        if row['TMD'] == -1 or row['CV'] == -1:
            return 'Zero consumption only'
        elif row['CV'] > CV_THRESHOLD and row['TMD'] > TMD_THRESHOLD:
            return 'Sporadic'
        elif row['CV'] > CV_THRESHOLD and row['TMD'] < TMD_THRESHOLD:
            return 'Intermittent'
        elif row['CV'] < CV_THRESHOLD and row['TMD'] > TMD_THRESHOLD:
            return 'Erratic'
        elif row['CV'] < CV_THRESHOLD and row['TMD'] < TMD_THRESHOLD:
            return 'Smooth'
        return 'Other'

    def get_reservations(material, n, t0028_df):
        reservas = t0028_df[t0028_df['Material'] == material]
        reservas = reservas[reservas['Com registro final'] == 'X']
        reservas = reservas[reservas['Tipo de reserva'] == 1]
        cols = ["Centro custo", "Data base", "Nome do usuário", "Cód. Localização", 
                "Descrição do Equipamento", "Motivo da Reserva", 'Qtd.retirada']
        return reservas[cols].fillna('-').head(n)

    def convert_text(material, t0127_df):
        text_fields = ['Texto - pt', 'Texto - es', 'Texto OBS - pt', 'Texto OBS - es',
                       'Texto DB - pt', 'Texto DB - es', 'Texto REF LMR']
        try:
            return [t0127_df[t0127_df['Material'] == material][field].str.cat(sep='\n') for field in text_fields]
        except Exception:
            return [''] * 7

    def calculate_PR(row):
        consumo = row[LT_COLUMNS][:LT_WINDOW]
        consumo = consumo[consumo > 0].sort_values(ascending=False)
        Q1 = consumo.quantile(0.25)
        Q3 = consumo.quantile(0.75)
        IQR = Q3 - Q1
        outliers = consumo[consumo > Q3 + 1.5 * IQR].tolist()
        consumo = consumo[consumo < Q3 + 1.5 * IQR]
        if len(consumo) == 0:
            pr = 1
        elif len(consumo) == 1:
            pr = consumo.iloc[0]
        else:
            pr = np.ceil(consumo.iloc[1] * (1 + GORDURA_PR/100))
        return pr, outliers

    def reservations_by_cost_center(row, t0028_df):
        r = get_reservations(row['Material'], 200, t0028_df)
        summary = (r[['Centro custo','Qtd.retirada']]
                  .groupby('Centro custo')
                  .sum()
                  .sort_values(by='Qtd.retirada', ascending=False)
                  .head(5)
                  .reset_index()
                  .to_string(index=False, header=False, justify='left')
                  .replace('   ', ' ').replace('  ', ' '))
        return summary

    def calculate_maximum(row):
        consumo = row[LT_COLUMNS][:LT_WINDOW]
        lt = row['Prz.entrg.prev.']
        if lt == 0:
            return 0
        if lt % 30 == 0:
            periods = int(DEMAND_WINDOW * 12 / (lt / 30))
            max_value = consumo.iloc[:periods].sum() / DEMAND_WINDOW
        else:
            row['pre-analise'] += f'| **Unexpected lead time of {lt} for {row["Material"]}**\n'
            max_value = 0
        return np.ceil(max_value * (1 + GORDURA_MAX/100))

    def define_recommendations(row):
        row['Image'] = f"P:\\Mfotos\\Padronizadas\\{row['Material']}(A).jpg"
        row['pre-analise'] = ''
        row['Classificação'] = classify(row)
        value_updated = False
        texts = convert_text(row['Material'], t0127)
        row['Texto - pt'], row['Texto - es'], row['Texto OBS - pt'], row['Texto OBS - es'], \
        row['Texto DB - pt'], row['Texto DB - es'], row['Texto REF LMR'] = texts
        row['Ajustes'] += validate_text_fields(row)
        if row['Grupo MRP'] == 'ZSTK':
            row = analyze_zstk_group(row, value_updated)
        elif row['Grupo MRP'] == 'SMIT':
            row['pre-analise'] += "| Verify JIRA query\n"
        elif row['Grupo MRP'] in ['FRAC', 'AD']:
            pass
        elif row['Grupo MRP'] == 'ANA':
            row['pre-analise'] += '| Material for ANALYSIS\n'
        else:
            row['pre-analise'] += f'| Check other materials in group {row["Grupo MRP"]} for replenishment in ZMM00124\n'
        if row['Tipo de MRP'] not in ['ZE','ZD','ZS','ZM','ZP','ZO','ZL']:
            row['pre-analise'] += '| ******* Policy ******* ' + row['Tipo de MRP']
        if 'sustentaveis' in row['Texto OBS - pt'].lower() or 'sustentáveis' in row['Texto OBS - pt'].lower():
            row['pre-analise'] += "| ANEXAR COMPRAS SUSTENTAVEIS\n"
        if 'desenho' in row['Texto OBS - pt'].lower():
            row['pre-analise'] += "| ANEXAR DESENHO\n"
        if row['Sld. Virtual'] > row['Qtd.ordem planejada'] and row['Dem. Pro.'] == True:
            row['pre-analise'] += '| Saldo virtual maior que qte em OP fazer reserva tipo 3 ou consumir da req/OC\n'
        if row['Sld. Virtual'] + row['Qtd.ordem planejada'] > row['Estoque máximo']:
            row['pre-analise'] += '| Compra acima do max\n'
        if not row['pre-analise']:
            row['pre-analise'] = '| No comments |'
        return row

    def validate_text_fields(row):
        adjustments = []
        if (row['Texto OBS - pt'] == "" and row['Texto OBS - es'] != "") or (row['Texto OBS - pt'] != "" and row['Texto OBS - es'] == ""):
            adjustments.append("Check OBS\n")
        if (row['Texto DB - pt'] == "" and row['Texto DB - es'] != "") or (row['Texto DB - pt'] != "" and row['Texto DB - es'] == ""):
            adjustments.append("Check DB\n")
        if row['Texto OBS - pt'] == row['Texto DB - pt'] and row['Texto OBS - pt'] != '':
            adjustments.append("OBS-PT same as DB-PT\n")
        if row['Texto OBS - es'] == row['Texto DB - es'] and row['Texto OBS - es'] != '':
            adjustments.append("OBS-ES same as DB-ES\n")
        if row['Nº peça fabricante'] == '':
            adjustments.append("Fill manufacturer part number")
        return '\n'.join(adjustments)

    def analyze_zstk_group(row, value_updated):
        if row['Grupo de mercadorias'] == "99" or len(row["Grupo de mercadorias"]) < 4:
            row['pre-analise'] += "Adjust merchandise group\n"
        if row['Classificação'] != 'Apenas consumo zero':
            row = analyze_active_material(row, value_updated)
        else:
            row['pre-analise'] += "| Consumo ZERO\n"
        return row

    def analyze_active_material(row, value_updated):
        if row['Dt. Ult. Pedido'] == -1:
            row['pre-analise'] += '| Never ordered\n'
        elif (datetime.now() - pd.to_datetime(row['Dt. Ult. Pedido'])).days > ANOS_SEM_OC * 360:
            row['pre-analise'] += f'| No orders in last {ANOS_SEM_OC} years\n'
        else:
            value_updated = True
        if row['Tipo de MRP'] == 'ZP' and row['Classificação'] != 'Suave':
            row['pre-analise'] += '| Change to ZM\n'
        if row['Volume da OP'] > ALTO_VOLUME:
            row['pre-analise'] += '| High OP volume, consider partial delivery\n'
        if row['Classificação'] == 'Errático' and row['Valor Total'] > ORDER_ZE_TRESHOLD and row['Criticidade'] > 0 and row['Tipo de MRP'] != 'ZE':
            row['pre-analise'] += '| Change to ZE\n'
        pr_value, row['outliers'] = calculate_PR(row)
        max_value = calculate_maximum_value(row, pr_value, value_updated)
        row = update_stock_levels(row, pr_value, max_value)
        row = analyze_stock_trends(row)
        return row

    def calculate_maximum_value(row, pr_value, value_updated):
        if row['Valor Total'] < BAIXO_VALOR:
            if row['Tipo de MRP'] == 'ZM':
                if value_updated:
                    return pr_value + 3 * calculate_maximum(row)
                else:
                    row['pre-analise'] += '| Low outdated value\n'
                    return max(pr_value + calculate_maximum(row), pr_value * 2)
            elif row['Tipo de MRP'] == 'ZE':
                if pr_value != row['Ponto reabastec.']:
                    row['pre-analise'] += f'| Suggested PR={pr_value}\n'
                return pr_value
        return calculate_maximum(row) + pr_value

    def update_stock_levels(row, pr_value, max_value):
        if row['Tipo de MRP'] != 'ZP' and row['Classificação'] == 'Suave':
            row['pre-analise'] += f'| Change to ZP, CMM = {np.ceil(max_value/12)}\n'
        else:
            if pr_value != row['Ponto reabastec.']:
                row['pre-analise'] += f'| Suggested PR={pr_value}\n'
            if max_value != row['Estoque máximo']:
                row['pre-analise'] += f'| Suggested MAX={max_value}\n'
        row['PR sug'] = pr_value
        row['MAX sug'] = max_value
        return row

    def analyze_stock_trends(row):
        mean_val = row[LT_COLUMNS[:4]].mean()
        if mean_val > 2:
            if row['1 LTD'] < mean_val and row['2 LTD'] < mean_val:
                row['tendencia'] = 'Downward trend'
            elif row['1 LTD'] > mean_val and row['2 LTD'] > mean_val:
                row['tendencia'] = 'Upward trend'
            else:
                row['tendencia'] = 'Stable'
        else:
            row['tendencia'] = '-'
        return row

    def consolidate_duplicates(df_in, id_column='Material', qty_column='Qtd.ordem planejada'):
        duplicate_mask = df_in.duplicated(subset=[id_column], keep=False)
        duplicates = df_in[duplicate_mask]
        aggregated = duplicates.groupby(id_column, as_index=False).agg({
            qty_column: 'sum',
            **{col: 'first' for col in df_in.columns if col not in [id_column, qty_column]}
        })
        aggregated['Dem. Pro.'] = True
        df_no_duplicates = df_in[~duplicate_mask]
        consolidated_df = pd.concat([df_no_duplicates, aggregated], ignore_index=True)
        return consolidated_df

    # Cálculo das colunas derivadas
    df = consolidate_duplicates(df)
    df['Dias em OP'] = (datetime.now() - df['Data abertura plan.']).dt.days
    df['TMD'] = df.apply(TMD, axis=1)
    df['CV'] = df.apply(CV, axis=1)
    df['reservas_por_ceco'] = df.apply(lambda x: reservations_by_cost_center(x, t0028), axis=1)
    df_final = df.apply(define_recommendations, axis=1)
    
    colunas = ['Material','pre-analise','Dias em OP','Txt.brv.material','Tipo de MRP','Ponto reabastec.',
               'Estoque máximo','PR sug','MAX sug','impacto $ da sugestao','Prz.entrg.prev.','Sld. Virtual',
               "Image",'Planejador MRP','Grupo de mercadorias','Grupo MRP','Criticidade','Classificação',
               'Nº peça fabricante','outliers','Valor Total','Qtd.ordem planejada','Preço Unit.',
               'Dt. Ult. Pedido','tendencia'] + \
              [f'{i} LTD' for i in range(1, 16)] + \
              ['reservas_por_ceco','Qtd. RTP1','Qtd. RTP3','Qtd. RTP6','Qtd. LMR',
               'Volume da OP','Setor de atividade','TMD','CV',"Dem. Pro.","Fornecedor","Nome",
               "Qtd. Pedido","Qtd. Requisição",'Texto - pt','Texto - es','Texto OBS - pt',
               'Texto OBS - es','Texto DB - pt','Texto DB - es','Texto REF LMR',
               'analise','Ajustes','disponivel','preco_mercado','matches_description','comentarios','url']
    df_final = df_final[colunas]
    
    # --- Integração com pesquisa de mercado via API Perplexity ---
    import requests

    def query_perplexity(prompt: str, content: str, api_key: str) -> Tuple[str, List]:
        url = "https://api.perplexity.ai/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "sonar",
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": content}
            ],
            "max_tokens": 500,
            "temperature": 0,
            "top_p": 0.9
        }
        try:
            response = requests.post(url, json=payload, headers=headers)
            response_json = response.json()
            if 'choices' in response_json and response_json['choices']:
                return (response_json['choices'][0]['message']['content'],
                        response_json.get('citations', []))
            return "[STATUS]:ERRO_API", []
        except Exception as e:
            return f"[STATUS]:ERRO|{str(e)}", []

    def parse_result(content: str) -> dict:
        result = {
            'url': None,
            'price': None,
            'matches_description': False,
            'availability': False,
            'comments': None
        }
        price_match = re.search(r'\[PREÇO\]:\|(.*?)\|', content)
        if price_match:
            try:
                price_str = price_match.group(1).replace('R$', '').strip()
                result['price'] = float(re.sub(r'[^\d.,]', '', price_str).replace(',', '.'))
            except ValueError:
                pass
        match_match = re.search(r'\[MATCH\]:\|(.*?)\|', content)
        if match_match:
            result['matches_description'] = match_match.group(1).strip().lower() == 'sim'
        avail_match = re.search(r'\[DISPONIVEL\]:\|(.*?)\|', content)
        if avail_match:
            result['availability'] = avail_match.group(1).strip().lower() == 'sim'
        comment_match = re.search(r'\[COMENTARIO\]:\|(.*?)\|', content)
        if comment_match:
            result['comments'] = comment_match.group(1).strip()
        return result

    def process_dataframe(df_in: pd.DataFrame, api_key: str, anos_sem_oc: int, periodo_pesquisa: int) -> pd.DataFrame:
        prompts = {
            'part_search': """Você é um assistente especializado em encontrar peças e componentes industriais e de uso geral.
Sua tarefa é localizar um produto com base no descritivo técnico.

Regras:
1. Procure apenas produtos que atendam TODOS os requisitos do descritivo
2. Retorne APENAS UM produto que melhor atenda às especificações
3. Confirme se o produto está disponível para compra
4. Forneça apenas as informações solicitadas no formato abaixo

Formato da resposta (use EXATAMENTE este formato):
[LINK]:|url_do_produto|
[PREÇO]:|valor_em_reais|
[DISPONIVEL]:|sim_ou_nao|
[COMENTARIO]:|explicacao_breve|""",
            'part_verification': """Você é um assistente especializado em verificar peças industriais e de uso geral.
Verifique se a referência fornecida corresponde ao descritivo técnico e sua disponibilidade.

Regras:
1. Verifique se a referência corresponde EXATAMENTE ao descritivo
2. Confirme se o produto está disponível para compra
3. Encontre o preço de mercado (se disponível)
4. Forneça apenas as informações solicitadas no formato abaixo

Formato da resposta (use EXATAMENTE este formato):
[MATCH]:|sim_ou_nao|
[DISPONIVEL]:|sim_ou_nao|
[PREÇO]:|valor_em_reais|
[COMENTARIO]:|explicacao_breve|"""
        }
        obsolescence = datetime.now() - timedelta(days=anos_sem_oc * 365)
        df_in['Dt. Ult. Pedido'] = pd.to_datetime(df_in['Dt. Ult. Pedido'], errors='coerce')
        for col in ['url', 'preco_mercado', 'matches_description', 'disponivel', 'comentarios']:
            df_in[col] = None
        mask = (df_in['Dias em OP'] < periodo_pesquisa) & ((df_in['Dt. Ult. Pedido'] < obsolescence) | pd.isna(df_in['Dt. Ult. Pedido']))
        print('-' * 10, 'Pesquisa de mercado:\n')
        print(f"Total rows: {len(df_in)}, Processing: {mask.sum()}")
        for idx, row in df_in[mask].iterrows():
            print(f"Processing {idx + 1}/{mask.sum()}")
            has_part_number = not pd.isna(row['Nº peça fabricante'])
            if has_part_number:
                response, citations = query_perplexity(
                    prompts['part_verification'],
                    f"Referência: {row['Nº peça fabricante']}\nDescritivo: {row['Texto - pt']}",
                    api_key
                )
            else:
                response, citations = query_perplexity(
                    prompts['part_search'],
                    row['Texto - pt'],
                    api_key
                )
            result = parse_result(response)
            df_in.at[idx, 'url'] = '\n'.join(citations) if citations else None
            df_in.at[idx, 'preco_mercado'] = result['price']
            df_in.at[idx, 'matches_description'] = result['matches_description']
            df_in.at[idx, 'disponivel'] = result['availability']
            df_in.at[idx, 'comentarios'] = result['comments']
            time.sleep(1)
        return df_in

    df_final = process_dataframe(df_final, api_key=perplexity_api_key, anos_sem_oc=ANOS_SEM_OC, periodo_pesquisa=PERIODO_PESQUISA)

    def compare(text_pt, text_es):
        time.sleep(1)
        MODEL = 'gemini'
        try:
            prompt = f"""
Analise cuidadosamente os textos em português e em espanhol, identificando e descrevendo erros de ortografia, gramática ou vocabulário em cada idioma, além de incoerências entre as versões (informações faltantes ou divergentes); então apresente correções sugeridas para cada ponto encontrado, garantindo consistência entre os dois textos.
Ignore caracteres especiais como ç,ñ e acentos e nao recomende inclusao deles.
Não adicione comentarios alem das sugestoes de modificação.
Caso nao haja alterações a serem feita, retorne -.

Texto em portugues:
{text_pt}

Texto em espanhol:
{text_es}

Exemplo de resposta
Portugues:
1. "MOLDADA" -> "CAIXA MOLDADA"
2. "TIPO DE CAIXA FIXACAO" -> "TIPO DE FIXACAO"
3. "INTERRUP" -> "INTERRUPCAO"

Espanhol:
1. "TIPO DECAJA MOLDEADA" -> "TIPO DE CAJA MOLDEADA" 
2. "AJUSTE ELEMENTO" -> "AJUSTE DEL ELEMENTO"

Divergencia:
1. "(CURVA C)" aparece na mesma linha de "ALTURA" no espanhol, mas em linha separada no português - padronizar em linhas separadas
"""
            if MODEL == "claude":
                message = anthropic_client.messages.create(
                    model="claude-3-5-sonnet-20241022",
                    max_tokens=500,
                    messages=[{"role": "user", "content": prompt}]
                )
                return message.content[0].text
            elif MODEL == "deepseek":
                response = deepseek_client.chat.completions.create(
                    model="deepseek-chat",
                    max_tokens=500,
                    messages=[{"role": "user", "content": prompt}],
                    stream=False
                )
                return response.choices[0].message.content
            elif MODEL == "openai":
                completion = openai_client.chat.completions.create(
                    model="o3-mini-2025-01-31",
                    max_tokens=500,
                    messages=[{"role": "user", "content": prompt}]
                )
                return completion.choices[0].message.content
            elif MODEL == 'gemini':
                message = model.generate_content(
                    prompt,
                    generation_config=genai.types.GenerationConfig(
                        candidate_count=1,
                        max_output_tokens=500,
                        temperature=1.0
                    )
                )
                return message.text
        except Exception as e:
            print("\n***** Error comparing ", e)
            time.sleep(30)
            return "\nSem analise de texto"

    def ajustments(row):
        if row['Dias em OP'] < PERIODO_ANALISE_TEXTO and row['Grupo MRP'] == 'ZSTK':
            texto_comp = compare(row['Texto - pt'], row['Texto - es'])
            obs_comp = compare(row['Texto OBS - pt'], row['Texto OBS - es'])
            if obs_comp == '-':
                row['Ajustes'] += f'TEXTO:\n{texto_comp}'
            if texto_comp == '-':
                row['Ajustes'] += f'OBS:\n{obs_comp}'
            print(f"\nAjustes do codigo: {row['Material']}\n{row['Ajustes']}")
        return row

    df_final = df_final.apply(ajustments, axis=1)

    # Exportar arquivo final de análise (usando um template do Excel)
    template_path = Path("Analise_Template.xlsm")
    output_file = data_folder / "Analise.xlsm"
    shutil.copy(template_path, output_file)
    with pd.ExcelWriter(output_file, engine='openpyxl', mode='a', if_sheet_exists='replace', engine_kwargs={'keep_vba': True}) as writer:
        df_final.to_excel(writer, sheet_name='DADOS', index=False, freeze_panes=(1,2))

    # Gerar arquivos separados por Setor de atividade
    OUTPUT_DIR = data_folder / "setores"
    OUTPUT_DIR.mkdir(exist_ok=True)
    for setor, grupo in df_final.groupby('Setor de atividade'):
        output_file_setor = OUTPUT_DIR / f'analise_{setor}.xlsm'
        shutil.copy(Path("Analise_Template.xlsm"), output_file_setor)
        with pd.ExcelWriter(output_file_setor, engine='openpyxl', mode='a', if_sheet_exists='replace', engine_kwargs={'keep_vba': True}) as writer:
            grupo.to_excel(writer, sheet_name='DADOS', index=False, freeze_panes=(1,2))
        print(f'Created file: {output_file_setor} with {len(grupo)} rows')

    def process_and_split_dataframe(filename):
        os.makedirs(data_folder / "grupos", exist_ok=True)
        grupos_folder = data_folder / "grupos"
        for file in os.listdir(grupos_folder):
            os.unlink(grupos_folder / file)
        df_split = pd.read_excel(data_folder / filename)
        df_filtered = df_split[df_split['analise'] == 'Repor'].copy()
        df_filtered['grupo_4dig'] = df_filtered['Grupo de mercadorias'].astype(str).str[:4]
        df_filtered['grupo_2dig'] = df_filtered['Grupo de mercadorias'].astype(str).str[:2]
        grupo_sums = df_filtered.groupby('grupo_4dig')['Valor Total'].sum().reset_index()
        small_groups = grupo_sums[grupo_sums['Valor Total'] < 1000]['grupo_4dig'].tolist()
        processed_groups = set()
        for grupo in grupo_sums[grupo_sums['Valor Total'] >= 1000]['grupo_4dig']:
            grupo_df = df_filtered[df_filtered['grupo_4dig'] == grupo]
            filename_grp = grupos_folder / f'grupo_{grupo}.xlsx'
            grupo_df.to_excel(filename_grp, index=False)
            processed_groups.add(grupo)
        for grupo in small_groups:
            if grupo in processed_groups:
                continue
            grupo_2dig = grupo[:2]
            mask = (df_filtered['grupo_2dig'] == grupo_2dig) & (~df_filtered['grupo_4dig'].isin(processed_groups))
            grupo_df = df_filtered[mask]
            if not grupo_df.empty:
                filename_grp = grupos_folder / f'grupo_{grupo_2dig}_combinado.xlsx'
                grupo_df.to_excel(filename_grp, index=False)
                processed_groups.update(grupo_df['grupo_4dig'].unique())
        return len(processed_groups)

    num_grupos = process_and_split_dataframe("setores/analise_31.xlsm")
    print(f'Foram geradas planilhas para {num_grupos} grupos diferentes.')

    # Exportar o resultado final em JSON
    df_red = df_final[df_final['Grupo MRP'] == "ZSTK"]
    json_data = df_red.to_json(orient="records", force_ascii=False, indent=4)
    with open(data_folder / "arquivo.json", "w", encoding='utf-8') as file:
        file.write(json_data)

    return df_final

if __name__ == '__main__':
    current_folder = Path(f"./data/{datetime.now().strftime('%Y-%m')}/")
    results = run_analysis(current_folder)
    print("Análise concluída.")
