import yfinance as yf
# Doc: https://ranaroussi.github.io/yfinance/

B3_EXCHANGE_CODE = '.SA'  # OBS: .SA é para todas ações brasileiras nessa biblioteca

def get_ativo(ticker, news_service):
    """
    Get ticker information, checking database first, then API if needed
    
    Args:
        ticker: Stock symbol (e.g., 'PETR4', 'VALE3')
        news_service: NewsService instance for database operations (optional)
    
    Returns:
        Dictionary with ticker information
    """
    print("=" * 60)
    print(f"Busca de ativo: {ticker}")
    print("=" * 60)

    # First, try to get ticker from database if news_service is provided
    db_ticker = news_service.get_ticker(ticker)
    
    if db_ticker:
        print("Ticker encontrado no banco de dados!")
        print(f"Nome: {db_ticker['nome_empresa']}")
        print(f"Setor: {db_ticker.get('setor', 'N/A')}")
        
        return db_ticker['id'], db_ticker['nome_empresa']
    
    # If not found in database, fetch from Yahoo Finance API
    print("Ticker não encontrado no banco. Buscando na API do Yahoo Finance...")
    
    ticker_yahoo = yf.Ticker(ticker + B3_EXCHANGE_CODE)
    info = ticker_yahoo.get_info()
    name = info.get("longName", "Unknown")
    
    if name == "Unknown":
        raise ValueError(f"Empresa não encontrado para: {ticker}")
    
    print("Empresa encontrada com sucesso na API!")
    name = name.removesuffix(" S.A.")
    print(name)

    sector = info.get("sector", "Unknown")
    if sector == "Unknown":
        print(f"Setor não encontrado!")
    else:
        print(f"Setor encontrado: {sector}")

    # Save to database if news_service is provided
    ticker_id = None
    print("Salvando ticker no banco de dados...")
    ticker_id = news_service.save_ticker(
        simbolo=ticker,
        nome_empresa=name,
        setor=sector,
        mercado='B3'
    )
        
    if ticker_id:
        print(f"Ticker salvo no banco com ID: {ticker_id}")
    else:
        print("Erro ao salvar ticker no banco de dados")

    return ticker_id, name


if __name__ == "__main__":
    # Test the get_ativo function
    from src.financial_analysis.services.news_service import NewsService
    
    # Create a mock news service for testing
    news_service = NewsService()
    
    # Test with a Brazilian stock ticker
    test_ticker = "EMBR3"
    
    try:
        print("Testing get_ativo function...")
        ticker_id, company_name = get_ativo(test_ticker, news_service)
        print(f"\nResult: ID={ticker_id}, Name={company_name}")
    except Exception as e:
        print(f"Error: {e}")