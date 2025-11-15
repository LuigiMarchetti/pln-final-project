import requests
from bs4 import BeautifulSoup
import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import RSLPStemmer
from nltk.stem import WordNetLemmatizer
import string, re
from urllib.parse import urljoin
from datetime import timedelta, datetime

from src.financial_analysis.services.news_service import get_news_service


# ---- Utilitários ----
def converter_nome_empresa_para_url(nome_empresa):
    """
    Converte o nome da empresa para o formato usado pela Exame em URLs.
    Exemplo: 'Banco do Brasil' -> 'banco-do-brasil'
    - Normaliza para minúsculas
    - Remove acentos
    - Troca espaços por hífens
    - Remove caracteres especiais
    """
    nome_lower = nome_empresa.lower()
    nome_limpo = re.sub(r'[àáâãäå]', 'a', nome_lower)
    nome_limpo = re.sub(r'[èéêë]', 'e', nome_limpo)
    nome_limpo = re.sub(r'[ìíîï]', 'i', nome_limpo)
    nome_limpo = re.sub(r'[òóôõö]', 'o', nome_limpo)
    nome_limpo = re.sub(r'[ùúûü]', 'u', nome_limpo)
    nome_limpo = re.sub(r'[ç]', 'c', nome_limpo)
    nome_url = re.sub(r'\s+', '-', nome_limpo)  # espaços -> hífens
    nome_url = re.sub(r'[^a-z0-9\-]', '', nome_url)  # remove símbolos
    return nome_url


# ---- NLTK ----
# Download silencioso de pacotes necessários do NLTK
nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)
nltk.download('rslp', quiet=True)
nltk.download('punkt_tab', quiet=True)
nltk.download('wordnet', quiet=True)
nltk.download('omw-1.4', quiet=True)

# Stopwords e processadores linguísticos
stop_words = set(stopwords.words('portuguese'))
stemmer = RSLPStemmer()
lemmatizer = WordNetLemmatizer()


def processar(texto):
    """
    Pipeline básico de PLN:
    - Limpeza de pontuação
    - Tokenização
    - Remoção de stopwords
    - Stemming (radical)
    - Lemmatização (forma canônica)
    Retorna dicionário com tokens, stems e lemmas.
    """
    texto = texto.lower()
    texto = texto.translate(str.maketrans("", "", string.punctuation))
    texto = re.sub(r'[^a-záéíóúâêîôûãõç ]', ' ', texto)
    tokens = word_tokenize(texto, language='portuguese')
    tokens = [t for t in tokens if t not in stop_words and len(t) > 2]
    stems = [stemmer.stem(t) for t in tokens]
    lemmas = [lemmatizer.lemmatize(t) for t in tokens]
    return {"tokens": tokens, "stems": stems, "lemmas": lemmas}


# ---- Scraping ----
headers = {"User-Agent": "Mozilla/5.0"}  # Header para evitar bloqueios no request


def parse_relative_date(texto):
    """
    Converte expressões como 'há 2 dias', 'há 5 horas', 'há 4 semanas', 'há um mês'
    em timedelta. Retorna None se não reconhecer o padrão.
    """
    texto = texto.lower().strip()

    # trata "um" ou "uma" como 1
    if "há um " in texto:
        texto = texto.replace("há um ", "há 1 ")
    elif "há uma " in texto:
        texto = texto.replace("há uma ", "há 1 ")

    match = re.search(r"há (\d+) (\w+)", texto)
    if not match:
        return None

    num = int(match.group(1))
    unidade = match.group(2)

    if "dia" in unidade:
        return timedelta(days=num)
    elif "hora" in unidade:
        return timedelta(hours=num)
    elif "min" in unidade:
        return timedelta(minutes=num)
    elif "semana" in unidade:
        return timedelta(weeks=num)
    elif "mês" in unidade or "mes" in unidade:  # cobre "mês" e "mes"
        return timedelta(days=num * 30)  # aproxima mês como 30 dias
    elif "ano" in unidade:
        return timedelta(days=num * 365)  # aproxima ano como 365 dias
    else:
        return None


# --- Conversor de data em português ---
def parse_portuguese_date(date_text):
    """
    Converte datas escritas em português para datetime.
    Exemplo: '10 de setembro de 2025 às 10h55'
    Retorna None se não conseguir converter.
    """
    if not date_text or date_text == "Sem data":
        return None

    meses = {
        'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4,
        'maio': 5, 'junho': 6, 'julho': 7, 'agosto': 8,
        'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12
    }

    try:
        date_text = re.sub(r'^Publicado em\s*', '', date_text, flags=re.IGNORECASE)
        date_text = date_text.strip()
        # Corrige espaçamentos quebrados pela Exame
        date_text = re.sub(r'às(\d)', r'às \1', date_text)
        date_text = re.sub(r'(\d)h(\d)', r'\1h\2', date_text)

        pattern = r'(\d+)\s+de\s+(\w+)\s+de\s+(\d{4})\s+às\s+(\d{1,2})h(\d{2})'
        match = re.search(pattern, date_text, re.IGNORECASE)

        if match:
            dia = int(match.group(1))
            mes_nome = match.group(2).lower()
            ano = int(match.group(3))
            hora = int(match.group(4))
            minuto = int(match.group(5))

            if mes_nome in meses:
                mes = meses[mes_nome]
                return datetime(ano, mes, dia, hora, minuto)

        return None
    except Exception as e:
        print(f"Erro ao converter data '{date_text}': {e}")
        return None


# --- Extrator de links de artigos com filtro de tempo ---
def get_article_links_period(page_url, base_url, dias_max=30):
    """
    Busca links e títulos de artigos de uma página da Exame.
    Filtra para manter apenas os publicados nos últimos X dias (padrão=7).
    """
    resp = requests.get(page_url, headers=headers)
    soup = BeautifulSoup(resp.text, "html.parser")

    links = []
    for card in soup.select("h3 a.touch-area[href]"):
        href = urljoin(base_url, card["href"])
        titulo = card.get_text(strip=True)

        # Busca informação de tempo (há X dias/horas)
        parent_div = card.find_parent("div")
        tempo_tag = parent_div.select_one("div p.title-small")
        if tempo_tag:
            delta = parse_relative_date(tempo_tag.get_text(strip=True))
            if delta is None or delta > timedelta(days=dias_max):
                continue  # ignora notícias antigas

        links.append((titulo, href))
    return links


# --- Extrator de conteúdo de um artigo ---
def get_article_content(url):
    """
    Coleta os principais elementos de um artigo:
    - Título
    - Manchete (headline)
    - Autor
    - Data de publicação
    - Corpo da notícia (texto limpo)
    """
    r = requests.get(url, headers=headers)
    sp = BeautifulSoup(r.text, "html.parser")

    # Título da notícia (fix: restringe ao bloco de notícia)
    titulo_tag = sp.select_one('#news-component > div:nth-of-type(1) > div:nth-of-type(2) > h1')
    titulo = titulo_tag.get_text(strip=True) if titulo_tag else "Sem título"

    # Manchete (subtítulo)
    manchete_tag = sp.select_one("h2.title-medium")
    manchete = manchete_tag.get_text(strip=True) if manchete_tag else titulo

    # Autor (links que começam com /autor/)
    autor_tag = sp.select_one('a[href^="/autor/"]')
    autor = autor_tag.get_text(strip=True) if autor_tag else None

    # Data de publicação
    data_tag = sp.select_one("#news-component > div:nth-child(2) > p")
    if data_tag:
        data_pub_text = data_tag.get_text(strip=True)
        data_pub_text = re.sub(r'\s+', ' ', data_pub_text)
        data_pub_text = re.sub(r'<!--\s*-->', '', data_pub_text)
        data_pub_text = data_pub_text.strip()

        data_pub = parse_portuguese_date(data_pub_text)
        if not data_pub:  # tenta relativo
            delta = parse_relative_date(data_pub_text)
            if delta:
                data_pub = datetime.now() - delta
    else:
        data_pub = None

    # Corpo da notícia
    corpo_div = sp.find("div", id="news-body")
    texto = ""
    if corpo_div:
        for ad in corpo_div.find_all(id=re.compile(r"^ads_|^banner_")):
            ad.decompose()  # remove anúncios
        for tabela in corpo_div.find_all("table"):
            tabela.decompose()  # remove tabelas
        paragrafos = corpo_div.find_all("p")
        texto = " ".join(p.get_text(" ", strip=True) for p in paragrafos)

    return {
        "titulo": titulo,
        "manchete": manchete,
        "autor": autor,
        "data": data_pub,
        "texto": texto,
        "link": url
    }


# --- Função principal ---
def web_scrapping(ticker_id, company_name, news_service=None, months_ago: int = 1):
    """
    Executa o scraping na Exame para uma empresa específica.
    - Monta URL da empresa
    - Percorre paginação coletando artigos recentes
    - Extrai conteúdo e processa com NLP (tokens, stems, lemmas)
    - Salva no banco (se news_service for fornecido) ou em CSV
    """
    company_url = converter_nome_empresa_para_url(company_name)
    base_url = f"https://exame.com/noticias-sobre/{company_url}/"

    print(f"Executando web scraping Exame para: {company_name}")
    print(f"URL base: {base_url}")

    dados = []
    dias_max = int(months_ago * 30)
    pagina = 1
    print(f"Searching news for the last {dias_max} days (from {months_ago} months)")
    while True:
        page_url = base_url if pagina == 1 else f"{base_url}{pagina}/"
        print(f"Coletando página {pagina}: {page_url}")
        artigos = get_article_links_period(page_url, base_url, dias_max=dias_max)
        if not artigos:
            break  # se não há artigos, encerra loop

        for titulo, link in artigos:
            try:
                artigo = get_article_content(link)
                artigo["pln"] = processar(artigo["texto"])  # NLP
                dados.append(artigo)
            except Exception as e:
                print("Erro no link", link, e)

        pagina += 1

    # Monta DataFrame com resultados
    df = pd.DataFrame(dados)
    if df.empty:
        print(f"No recent news articles found for {company_name} on Exame.")
        return True

    df_tokens = pd.DataFrame({
        "titulo": df["titulo"],
        "manchete": df["manchete"],
        "autor": df["autor"],
        "data": df["data"],
        "link": df["link"],
        "texto_completo": df["texto"],
        "tokens": df["pln"].apply(lambda x: x["tokens"]),
        "stems": df["pln"].apply(lambda x: x["stems"]),
        "lemmas": df["pln"].apply(lambda x: x["lemmas"]),
    })

    print(df_tokens.head())

    # Se tiver news_service, salva no banco
    if news_service:
        print("💾 Saving data to database...")
        saved_count = 0
        for _, row in df_tokens.iterrows():
            try:
                # Dados básicos da notícia
                news_data = {
                    'url': row['link'],
                    'data_publicacao': row['data'] if isinstance(row['data'], datetime) else None,
                    'autor': row['autor'],
                    'tipo_fonte': 'EXAME',
                    'categoria': None,
                    'sentimento': None,
                    'relevancia': int(0),  # 👈 força inteiro em vez de float
                }
                ticker_id = int(ticker_id) if ticker_id is not None else None

                # Processamento textual (título, manchete e corpo)
                titulo_processed = processar(row['titulo'])
                manchete_processed = processar(row['manchete'])
                corpo_processed = processar(row['texto_completo'])

                text_processing_data = {
                    'titulo': {
                        'texto_bruto': row['titulo'],
                        'tokens_normalizados': ' '.join(titulo_processed['tokens']) if titulo_processed[
                            'tokens'] else None,
                        'tokens_stemming': ' '.join(titulo_processed['stems']) if titulo_processed['stems'] else None,
                        'tokens_lemma': ' '.join(titulo_processed['lemmas']) if titulo_processed['lemmas'] else None,
                    },
                    'manchete': {
                        'texto_bruto': row['manchete'],
                        'tokens_normalizados': ' '.join(manchete_processed['tokens']) if manchete_processed[
                            'tokens'] else None,
                        'tokens_stemming': ' '.join(manchete_processed['stems']) if manchete_processed[
                            'stems'] else None,
                        'tokens_lemma': ' '.join(manchete_processed['lemmas']) if manchete_processed[
                            'lemmas'] else None,
                    },
                    'corpo': {
                        'texto_bruto': row['texto_completo'],
                        'tokens_normalizados': ' '.join(corpo_processed['tokens']) if corpo_processed[
                            'tokens'] else None,
                        'tokens_stemming': ' '.join(corpo_processed['stems']) if corpo_processed['stems'] else None,
                        'tokens_lemma': ' '.join(corpo_processed['lemmas']) if corpo_processed['lemmas'] else None,
                    }
                }

                # Salva no banco via service externo
                ticker_id, news_id, processing_ids, is_new_news = news_service.save_complete_news_data_with_ticker_id(
                    ticker_id=ticker_id,
                    news_data=news_data,
                    text_processing_data=text_processing_data
                )

                #vectorization.vectorize_text_by_type(row['titulo'], 'BERT', processing_ids[0])
                #vectorization.vectorize_text_by_type(row['manchete'], 'BERT', processing_ids[1])
                #vectorization.vectorize_text_by_type(row['corpo'], 'BERT', processing_ids[2])

                if ticker_id and news_id:
                    if is_new_news:
                        saved_count += 1
                    else:
                        # News already existed, just log it
                        print(f"News already exists: {row['link']}")

            except Exception as e:
                print(f"Error saving news to database: {e}")
                continue

        print(f"✅ Saved {saved_count} news articles to database")
    else:
        # Se não houver banco, salva CSV
        filename = f"exame_{company_url}_ultimos_7_dias.csv"
        df_tokens.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"✅ Concluído! Notícias dos últimos 7 dias salvas em {filename}.")

    return True


# Execução direta (modo standalone)
if __name__ == "__main__":
    # Executa exemplo padrão para Banco do Brasil
    web_scrapping("BUY_&_HOLD", "EMBR3", "1", "Embraer", get_news_service())
