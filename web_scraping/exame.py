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

from utils.database import initialize_database
from utils.news_service import get_news_service


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
    if not isinstance(texto, str):
        texto = str(texto)

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

def parse_relative_date(texto) -> timedelta | None:
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

    try:
        num = int(match.group(1))
        unidade = match.group(2)
    except (IndexError, ValueError):
        return None

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
def parse_portuguese_date(date_text: str) -> datetime | None:
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
        date_text = re.sub(r'^(Publicado|Atualizado)\s+em\s*', '', date_text, flags=re.IGNORECASE)
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

        # Tentar padrão sem hora
        pattern_sem_hora = r'(\d+)\s+de\s+(\w+)\s+de\s+(\d{4})'
        match_sem_hora = re.search(pattern_sem_hora, date_text, re.IGNORECASE)
        if match_sem_hora:
            dia = int(match_sem_hora.group(1))
            mes_nome = match_sem_hora.group(2).lower()
            ano = int(match_sem_hora.group(3))
            if mes_nome in meses:
                mes = meses[mes_nome]
                return datetime(ano, mes, dia) # Default to 00:00

        return None
    except Exception as e:
        print(f"Erro ao converter data '{date_text}': {e}")
        return None


# --- Extrator de links de artigos com filtro de tempo ---
def get_article_links_period(page_url, base_url, limit_date: datetime):
    """
    Busca links e títulos de artigos de uma página da Exame.
    Filtra para manter apenas os publicados APÓS a limit_date.
    Retorna (lista_de_links, continuar_paginando)
    """
    try:
        resp = requests.get(page_url, headers=headers, timeout=10)
        resp.raise_for_status() # Lança erro se status não for 200
    except requests.RequestException as e:
        print(f"Erro ao buscar URL {page_url}: {e}")
        return [], False # Parar paginação se houver erro

    soup = BeautifulSoup(resp.text, "html.parser")

    links = []
    continuar_paginando = True # Assume que deve continuar
    cards = soup.select("h3 a.touch-area[href]")

    if not cards:
        print("Nenhum card de notícia encontrado. Encerrando paginação.")
        return [], False # Para se não achar mais cards

    for card in cards:
        href = urljoin(base_url, card["href"])
        titulo = card.get_text(strip=True)

        # Busca informação de tempo (há X dias/horas)
        parent_div = card.find_parent("div")
        tempo_tag = parent_div.select_one("div p.title-small")

        if tempo_tag:
            delta = parse_relative_date(tempo_tag.get_text(strip=True))
            if delta:
                data_noticia = datetime.now() - delta
                if data_noticia < limit_date:
                    continuar_paginando = False # Encontrou notícia antiga
                    continue # Pula esta notícia e para de paginar

            links.append((titulo, href))
        else:
            # Se não achar data relativa, adiciona mesmo assim (pode ser antiga)
            # Mas é melhor pecar por excesso
            links.append((titulo, href))

    return links, continuar_paginando


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
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"Erro ao buscar conteúdo do artigo {url}: {e}")
        return None # Retorna None se falhar

    sp = BeautifulSoup(r.text, "html.parser")

    # Título principal (às vezes indexado no [1])
    titulo_tag = sp.select_one("h1") or sp.select_one("header h1") or sp.find("h1")
    titulo = titulo_tag.get_text(strip=True) if titulo_tag else "Sem título"

    # Manchete (subtítulo)
    manchete_tag = sp.select_one("h2.title-medium")
    manchete = manchete_tag.get_text(strip=True) if manchete_tag else titulo

    # Autor (links que começam com /autor/)
    autor_tag = sp.select_one('a[href^="/autor/"]')
    autor = autor_tag.get_text(strip=True) if autor_tag else None

    # Data de publicação
    data_pub = None
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
        # Tenta outra tag de data (mais comum)
        data_tag_alternativa = sp.select_one('p[class*="meta-post-date"]')
        if data_tag_alternativa:
            data_pub_text = data_tag_alternativa.get_text(strip=True)
            data_pub = parse_portuguese_date(data_pub_text)
            if not data_pub:
                delta = parse_relative_date(data_pub_text)
                if delta:
                    data_pub = datetime.now() - delta

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
def web_scrapping(ticker: str, ticker_id: int, company_name: str, news_service=None, months_ago: int = 1):
    """
    Executa o scraping na Exame para uma empresa específica.
    - Monta URL da empresa
    - Percorre paginação coletando artigos recentes
    - Extrai conteúdo e processa com NLP (tokens, stems, lemmas)
    - Salva no banco (se news_service for fornecido) ou em CSV
    """
    company_url = converter_nome_empresa_para_url(company_name)
    base_url = f"https://exame.com/noticias-sobre/{company_url}/"

    # Calcula data limite
    dias_atras = months_ago * 30 # Aproximação de 30 dias/mês
    limit_date = datetime.now() - timedelta(days=dias_atras)

    print(f"Executando web scraping Exame para: {company_name}")
    print(f"URL base: {base_url}")
    print(f"Buscando notícias desde: {limit_date.strftime('%Y-%m-%d')}")

    dados = []
    pagina = 1
    continuar_paginando = True

    while continuar_paginando:
        page_url = base_url if pagina == 1 else f"{base_url}{pagina}/"
        print(f"Coletando página {pagina}: {page_url}")

        artigos, continuar_paginando = get_article_links_period(page_url, base_url, limit_date=limit_date)

        if not artigos and pagina == 1:
            print(f"Nenhum artigo encontrado para {company_name} na Exame.")
            break # Se não há artigos na primeira página, encerra

        for titulo, link in artigos:
            try:
                artigo = get_article_content(link)
                if not artigo:
                    print(f"Falha ao obter conteúdo do link: {link}")
                    continue

                # Filtro final pela data (se disponível no artigo)
                if artigo["data"] and artigo["data"] < limit_date:
                    continuar_paginando = False # Garante parada se data exata for antiga
                    continue

                artigo["pln"] = processar(artigo["texto"])  # NLP
                dados.append(artigo)
            except Exception as e:
                print("Erro no link", link, e)

        pagina += 1
        if pagina > 50: # Limite de segurança para não ficar em loop infinito
            print("Atingiu limite de 50 páginas. Encerrando.")
            break

    if not dados:
        print(f"Nenhuma notícia recente encontrada para {company_name} na Exame.")
        return True # Sucesso, mas sem dados

    # Monta DataFrame com resultados
    df = pd.DataFrame(dados)
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

    print(f"Encontradas {len(df_tokens)} notícias na Exame.")
    # print(df_tokens.head())

    # Se tiver news_service, salva no banco
    if news_service:
        print("💾 Saving data to database...")
        saved_count = 0
        for _, row in df_tokens.iterrows():
            try:
                # Dados básicos da notícia
                news_data = {
                    'url': row['link'],
                    'data_publicacao': row['data'] if pd.notna(row['data']) else None,
                    'autor': row['autor'],
                    'tipo_fonte': 'EXAME',
                    'categoria': None,
                    'sentimento': None,
                    'relevancia': int(0),  # 👈 força inteiro em vez de float
                }

                # Garantir que ticker_id é int
                current_ticker_id = int(ticker_id) if ticker_id is not None else None
                if not current_ticker_id:
                    print(f"Erro: Ticker ID nulo para {ticker}. Pulando salvamento.")
                    continue

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
                print(f"Error saving news to database: {e} (Link: {row['link']})")
                continue

        print(f"✅ Saved {saved_count} new news articles to database from Exame")
    else:
        # Se não houver banco, salva CSV
        filename = f"exame_{company_url}_ultimos_{months_ago}_meses.csv"
        df_tokens.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"✅ Concluído! Notícias salvas em {filename}.")

    return True


# Execução direta (modo standalone)
if __name__ == "__main__":
    # Executa exemplo padrão para 3 meses
    print("Executando Exame Scraper em modo standalone...")
    ns = get_news_service()
    if not initialize_database():
        print("❌ Falha ao inicializar banco. Verifique conexão MySQL (e .env).")
    else:
        print("Buscando Ticker...")
        test_ticker = "EMBR3"
        tid, cname = get_news_service().save_or_get_ticker(test_ticker, "Embraer", "Industrial", "B3")
        if tid and cname:
            print(f"Ticker encontrado/criado: ID {tid}, Nome {cname}")
            web_scrapping(ticker=test_ticker, ticker_id=tid, company_name=cname, news_service=ns, months_ago=3)
        else:
            print(f"Não foi possível obter ticker {test_ticker}")