# -*- coding: utf-8 -*-
"""
Web Scraping InfoMoney - Coleta de notícias
-------------------------------------------
Fluxo:
1. Converte nome da empresa para URL do InfoMoney
2. Abre página "tudo-sobre" no Selenium
3. Clica em "Carregar mais" até encontrar notícia mais antiga que o limite (months_ago)
4. Extrai links + datas dos cards
5. Para cada link, coleta título, manchete, texto, data
6. Pré-processa textos (tokens, stems, lemmas)
7. Salva no banco usando news_service
"""

import time
import re
import string
from datetime import datetime, timedelta

import requests
import pandas as pd
from bs4 import BeautifulSoup

# --- Selenium ---
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException

# --- NLP ---
import spacy
import nltk
from nltk.corpus import stopwords
from nltk.stem import RSLPStemmer
from nltk.tokenize import word_tokenize

# --- Utils próprios ---
from utils import yahoo_finance
from utils.database import initialize_database
from utils.news_service import get_news_service

# ------------------- Configuração NLP -------------------

# Download de recursos NLTK (executa apenas uma vez)
try:
    nltk.download('rslp', quiet=True)
    nltk.download('stopwords', quiet=True)
    nltk.download('punkt', quiet=True)
    nltk.download('punkt_tab', quiet=True)
except Exception as e:
    print(f"Aviso: Falha ao baixar pacotes NLTK (pode ser problema de permissão ou rede): {e}")


# Carrega modelo SpaCy em português
try:
    nlp = spacy.load("pt_core_news_sm")
except OSError:
    print("⚠️ Modelo pt_core_news_sm não encontrado. Execute: python -m spacy download pt_core_news_sm")
    nlp = None

# Stopwords e Stemmer
stop_words = set(stopwords.words('portuguese'))
stemmer = RSLPStemmer()

# ------------------- Funções Utilitárias -------------------

def converter_nome_empresa_para_url(nome_empresa: str) -> str:
    """
    Converte nome da empresa no formato usado pelo InfoMoney.
    Ex.: 'Banco do Brasil' -> 'banco-do-brasil'
    """
    nome_lower = nome_empresa.lower()
    nome_limpo = re.sub(r'[àáâãäå]', 'a', nome_lower)
    nome_limpo = re.sub(r'[èéêë]', 'e', nome_limpo)
    nome_limpo = re.sub(r'[ìíîï]', 'i', nome_limpo)
    nome_limpo = re.sub(r'[òóôõö]', 'o', nome_limpo)
    nome_limpo = re.sub(r'[ùúûü]', 'u', nome_limpo)
    nome_limpo = re.sub(r'[ç]', 'c', nome_limpo)
    nome_url = re.sub(r'\s+', '-', nome_limpo)       # espaços -> hífen
    nome_url = re.sub(r'[^a-z0-9\-]', '', nome_url)  # remove caracteres especiais
    return nome_url

def texto_relativo_para_data_simples(texto: str):
    """
    Converte '... 5 dias atrás' ou '... 12 horas atrás' em datetime.
    Procura o número imediatamente antes de 'atrás'.
    """
    if not texto:
        return None

    agora = datetime.now()
    s = texto.lower()

    m = re.search(r'(\d+)\s*(horas?|dias?|semanas?|meses?|anos?)\s+atrás', s)
    if not m:
        # Tenta data absoluta "dd Mês yyyy"
        meses = {
            'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4,
            'mai': 5, 'jun': 6, 'jul': 7, 'ago': 8,
            'set': 9, 'out': 10, 'nov': 11, 'dez': 12
        }
        m_abs = re.search(r'(\d{1,2})\s+(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)\s+(\d{4})', s)
        if m_abs:
            try:
                dia = int(m_abs.group(1))
                mes = meses[m_abs.group(2)]
                ano = int(m_abs.group(3))
                return datetime(ano, mes, dia)
            except Exception:
                return agora # Fallback
        return agora  # fallback se não achar data relativa nem absoluta

    try:
        quantidade = int(m.group(1))
        unidade = m.group(2)
    except (IndexError, ValueError):
        return agora # Fallback

    if 'hora' in unidade:
        delta = timedelta(hours=quantidade)
    elif 'dia' in unidade:
        delta = timedelta(days=quantidade)
    elif 'semana' in unidade:
        delta = timedelta(weeks=quantidade)
    elif 'mes' in unidade: # meses
        delta = timedelta(days=30 * quantidade)
    elif 'ano' in unidade:
        delta = timedelta(days=365 * quantidade)
    else:
        delta = timedelta(days=quantidade) # Fallback

    return agora - delta

def coletar_links_e_datas(soup: BeautifulSoup):
    """
    Coleta os links e datas (convertidas) dos cards de notícias.
    """
    # Procura por ambos os tipos de card
    cards = soup.select('div[data-ds-component="card-sm"], div[data-ds-component="card-default"]')
    lista = []

    for card in cards:
        a = card.find("a", href=True)
        if not a:
            continue

        link = a['href']
        # Garantir URL completa
        if not link.startswith('http'):
            link = f"https://www.infomoney.com.br{link}"


        # Busca div com 'atrás' ou data absoluta
        tempo_texto = ""
        # Tenta tag 'time'
        time_tag = card.find("time")
        if time_tag:
            tempo_texto = time_tag.get_text(strip=True).lower()
        else:
            # Tenta divs inline (plano B)
            for d in card.find_all("div", class_="inline-flex"):
                t = d.get_text(strip=True).lower()
                if "atrás" in t or any(m in t for m in ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']):
                    tempo_texto = t
                    break

        data_pub = texto_relativo_para_data_simples(tempo_texto)
        lista.append({"link": link, "data": data_pub})

    return lista

def processar(texto: str):
    """
    Pré-processamento para NLP:
    - minúsculas
    - remove pontuação e caracteres especiais
    - tokenização
    - remove stopwords
    - stemming
    - lematização (se SpaCy disponível)
    """
    if not isinstance(texto, str):
        texto = str(texto)

    texto = texto.lower()
    texto = texto.translate(str.maketrans("", "", string.punctuation))
    texto = re.sub(r'[^a-záéíóúâêîôûãõç ]', ' ', texto)

    tokens = word_tokenize(texto, language='portuguese')
    tokens = [t for t in tokens if t not in stop_words and len(t) > 2]
    stems = [stemmer.stem(t) for t in tokens]

    if nlp:
        try:
            doc = nlp(" ".join(tokens))
            lemmas = [token.lemma_ for token in doc]
        except Exception as e:
            print(f"Erro no processamento SpaCy (texto pode ser muito longo): {e}")
            lemmas = stems # Fallback para stems
    else:
        lemmas = stems  # fallback

    return {"tokens": tokens, "stems": stems, "lemmas": lemmas}

def get_article_content(url: str):
    """
    Extrai título, manchete, data e corpo do artigo do InfoMoney.
    """
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"Erro ao buscar conteúdo do artigo {url}: {e}")
        return None # Retorna None se falhar

    sp = BeautifulSoup(r.text, "html.parser")

    titulo_tag = sp.find("div", {"data-ds-component": "article-title"})
    titulo = titulo_tag.find("h1").get_text(strip=True) if titulo_tag else ""

    manchete_tag = titulo_tag.find("div") if titulo_tag else None
    manchete = manchete_tag.get_text(strip=True) if manchete_tag else ""

    article_tag = sp.find("article", {"data-ds-component": "article"})
    paragrafos = article_tag.find_all("p") if article_tag else []
    texto = " ".join([p.get_text(strip=True) for p in paragrafos])

    data_pub = None
    data_tag = sp.find("time")
    if data_tag:
        data_pub_str = data_tag.get_text(strip=True) if data_tag else ""
        try:
            # Tenta formato "dd/mm/YYYY HhMM"
            data_pub = datetime.strptime(data_pub_str, "%d/%m/%Y %Hh%M")
        except ValueError:
            try:
                # Tenta formato "dd Mês YYYY HhMM"
                meses = {
                    'janeiro': 'Jan', 'fevereiro': 'Feb', 'março': 'Mar', 'abril': 'Apr',
                    'maio': 'May', 'junho': 'Jun', 'julho': 'Jul', 'agosto': 'Aug',
                    'setembro': 'Sep', 'outubro': 'Oct', 'novembro': 'Nov', 'dezembro': 'Dec'
                }
                # Normaliza "de" e "às"
                data_pub_str = data_pub_str.lower().replace(" de ", " ").replace(" às ", " ")
                # Abrevia meses
                for k, v in meses.items():
                    data_pub_str = data_pub_str.replace(k, v)
                data_pub = datetime.strptime(data_pub_str, "%d %b %Y %Hh%M")
            except Exception:
                print(f"Não foi possível converter data: {data_pub_str}")
                data_pub = None


    return {"titulo": titulo, "manchete": manchete, "data": data_pub, "texto": texto, "link": url}

def save(df_tokens: pd.DataFrame, news_service, ticker_id: int):
    """
    Salva as notícias processadas no banco via serviço.
    Processa título, manchete e corpo com NLP antes de salvar.
    """
    if not news_service:
        return

    print("💾 Salvando dados no banco de dados...")
    saved_count = 0

    for _, row in df_tokens.iterrows():
        try:
            news_data = {
                'url': row['link'],
                'data_publicacao': row['data'] if pd.notna(row['data']) else None,
                'autor': None, # InfoMoney não expõe autor facilmente
                'tipo_fonte': 'INFO_MONEY',
                'categoria': None,
                'sentimento': None,
                'relevancia': int(0) # Força int
            }

            # Garantir que ticker_id é int
            current_ticker_id = int(ticker_id) if ticker_id is not None else None
            if not current_ticker_id:
                print(f"Erro: Ticker ID nulo. Pulando salvamento.")
                continue

            # Pré-processamento
            titulo_processed = processar(row['titulo'])
            manchete_processed = processar(row['manchete'])
            corpo_processed = processar(row['texto'])

            text_processing_data = {
                'titulo': {
                    'texto_bruto': row['titulo'],
                    'tokens_normalizados': ' '.join(titulo_processed['tokens']),
                    'tokens_stemming': ' '.join(titulo_processed['stems']),
                    'tokens_lemma': ' '.join(titulo_processed['lemmas']),
                },
                'manchete': {
                    'texto_bruto': row['manchete'],
                    'tokens_normalizados': ' '.join(manchete_processed['tokens']),
                    'tokens_stemming': ' '.join(manchete_processed['stems']),
                    'tokens_lemma': ' '.join(manchete_processed['lemmas']),
                },
                'corpo': {
                    'texto_bruto': row['texto'],
                    'tokens_normalizados': ' '.join(corpo_processed['tokens']),
                    'tokens_stemming': ' '.join(corpo_processed['stems']),
                    'tokens_lemma': ' '.join(corpo_processed['lemmas']),
                }
            }

            _, _, _, is_new_news = news_service.save_complete_news_data_with_ticker_id(
                ticker_id=current_ticker_id,
                news_data=news_data,
                text_processing_data=text_processing_data
            )

            if is_new_news:
                saved_count += 1
            else:
                # News already existed, just log it
                # print(f"News already exists: {row['link']}")
                pass

        except Exception as e:
            print(f"Erro ao salvar notícia: {e} (Link: {row['link']})")
            continue

    print(f"✅ {saved_count} notícias novas salvas no banco de dados (InfoMoney)")
    return True

# ------------------- Função Principal -------------------

def web_scrapping(ticker: str, ticker_id: int, company_name: str, news_service=None, months_ago: int = 1):
    """
    Fluxo principal:
    - Converte nome para URL
    - Abre página InfoMoney
    - Carrega mais enquanto notícias dentro do limite
    - Extrai dados de cada artigo
    - Salva no banco
    """
    name_to_url = converter_nome_empresa_para_url(company_name)
    base_url = f"https://www.infomoney.com.br/tudo-sobre/{name_to_url}"

    # Calcula data limite
    dias_atras = months_ago * 30 # Aproximação
    limit_date = datetime.now() - timedelta(days=dias_atras)

    print(f"Executando web scraping InfoMoney para: {company_name}")
    print(f"URL base: {base_url}")
    print(f"Buscando notícias desde: {limit_date.strftime('%Y-%m-%d')}")

    try:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless') # Rodar sem abrir janela
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument("window-size=1920,1080")
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"Erro ao inicializar o Selenium (ChromeDriver): {e}")
        print("Verifique se o ChromeDriver está instalado e acessível no PATH do sistema.")
        return False


    try:
        driver.get(base_url)
        wait = WebDriverWait(driver, 10) # Espera de 10 seg
    except Exception as e:
        print(f"Erro ao abrir a URL {base_url}: {e}")
        driver.quit()
        return False

    todos_links = []
    try:
        while True:
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")

            # Coleta links + datas atuais
            novos_links = coletar_links_e_datas(soup)

            if not novos_links:
                print("Nenhum link encontrado na página.")
                break

            # Adiciona apenas links que ainda não foram coletados
            links_pagina_atual = []
            for item in novos_links:
                if item not in todos_links:
                    todos_links.append(item)
                    links_pagina_atual.append(item)

            # Se não adicionou nenhum link novo, paramos
            if not links_pagina_atual:
                print("Não foram encontrados links novos nesta página. Encerrando.")
                break

            # Verifica se existe alguma notícia mais antiga que o limite
            mais_antiga = min([item["data"] for item in links_pagina_atual if item["data"] is not None], default=datetime.now())
            if mais_antiga < limit_date:
                print(f"⛔ Encontrada notícia de {mais_antiga.strftime('%Y-%m-%d')}. Parando de carregar mais.")
                break

            # Tenta clicar em "Carregar mais"
            try:
                # Espera o botão ser clicável
                botao = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Carregar mais')]")))

                # Scroll até o botão e clica via JS
                driver.execute_script("arguments[0].scrollIntoView(true);", botao)
                time.sleep(1) # Espera o scroll
                driver.execute_script("arguments[0].click();", botao)
                print("Clicou em 'Carregar mais'...")
                time.sleep(3) # Espera o conteúdo carregar

            except (TimeoutException, ElementClickInterceptedException):
                print("⛔ Não há mais botão 'Carregar mais' ou ele não é clicável.")
                break
            except Exception as e:
                print(f"Erro inesperado ao clicar no botão: {e}")
                break
    finally:
        driver.quit()

    # Filtra todos os links coletados pela data limite
    dentro_limite = [item for item in todos_links if item["data"] and item["data"] >= limit_date]

    if not dentro_limite:
        print(f"Nenhuma notícia recente encontrada para {company_name} na InfoMoney.")
        return True # Sucesso, mas sem dados

    print(f"Coletados {len(dentro_limite)} links de notícias dentro do período.")

    dados = []
    for d in dentro_limite:
        try:
            content = get_article_content(d["link"])
            if content:
                # Usa a data do artigo (mais precisa) se disponível
                if content["data"]:
                    d["data"] = content["data"]

                # Filtra novamente se a data precisa do artigo for antiga
                if content["data"] and content["data"] < limit_date:
                    continue

                dados.append(content)
        except Exception as e:
            print("Erro no link", d["link"], e)

    if not dados:
        print(f"Nenhuma notícia recente encontrada para {company_name} na InfoMoney (após extração).")
        return True

    df = pd.DataFrame(dados)
    print(f"Encontradas {len(df)} notícias na InfoMoney.")
    # print(df.head())

    return save(df, news_service, ticker_id)

# ------------------- Execução Direta -------------------

if __name__ == "__main__":
    print("Executando InfoMoney Scraper em modo standalone...")
    news_service = get_news_service()
    if not initialize_database():
        print("❌ Falha ao inicializar banco. Verifique conexão MySQL (e .env).")
    else:
        print("Buscando Ticker...")
        test_ticker = "MGLU3"
        ticker_id, company_name = yahoo_finance.get_ativo(test_ticker, get_news_service())

        if ticker_id and company_name:
            print(f"Ticker encontrado/criado: ID {ticker_id}, Nome {company_name}")
            web_scrapping(ticker=test_ticker, ticker_id=ticker_id, company_name=company_name, news_service=news_service, months_ago=3)
        else:
            print(f"Não foi possível obter ticker {test_ticker}")