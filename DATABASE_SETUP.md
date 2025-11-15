# Guia de Configuração do Banco de Dados

Este guia ajudará você a configurar o banco de dados MySQL para a aplicação de web scraping de notícias de investimentos.

## Pré-requisitos

1. Servidor MySQL instalado e em execução
2. Dependências Python instaladas: `pip install -r requirements.txt`

## Configuração do Banco de Dados

### 1. Criar Usuário do Banco de Dados (Opcional mas Recomendado)

```sql
CREATE USER 'news_user'@'localhost' IDENTIFIED BY 'sua_senha_segura';
GRANT ALL PRIVILEGES ON investment_news.* TO 'news_user'@'localhost';
FLUSH PRIVILEGES;
```

### 2. Configuração do Banco de Dados

A aplicação usa valores de configuração fixos do banco de dados. Se você precisar modificar essas configurações, edite o dicionário `DB_CONFIG` em `utils/database.py`:

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'root',  # Altere isso para sua senha root do MySQL
    'database': 'investment_news',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci',
    'autocommit': True
}
```

**Importante**: Atualize o campo `password` para corresponder à sua senha root do MySQL ou crie um usuário dedicado conforme mostrado na etapa 1.

### 3. Executar a Aplicação

A aplicação irá automaticamente:
- Conectar ao MySQL
- Criar o banco de dados `investment_news` se ele não existir
- Criar todas as tabelas necessárias com o esquema apropriado
- Inicializar a conexão com o banco de dados

```bash
python app.py BUY_&_HOLD
```

## Esquema do Banco de Dados

A aplicação cria três tabelas principais:

### 1. `tickers` - Símbolos de ações e informações da empresa
- `id`: Chave primária
- `simbolo`: Símbolo da ação (ex: PETR4, VALE3, AAPL)
- `nome_empresa`: Nome da empresa
- `setor`: Setor da empresa
- `mercado`: Tipo de mercado (B3, NYSE, NASDAQ, OUTROS)
- `ativo`: Status ativo
- `data_criacao`: Timestamp de criação

### 2. `noticias` - Artigos de notícias
- `id`: Chave primária
- `ticker_id`: Chave estrangeira para a tabela tickers
- `url`: URL única da notícia
- `data_publicacao`: Data de publicação
- `autor`: Autor do artigo
- `tipo_fonte`: Tipo de fonte (EXAME, INFO_MONEY)
- `categoria`: Categoria da notícia
- `sentimento`: Análise de sentimento (POSITIVO, NEUTRO, NEGATIVO)
- `relevancia`: Pontuação de relevância (0.00 a 1.00)
- `data_criacao`: Timestamp de criação
- `data_atualizacao`: Timestamp da última atualização

### 3. `processamento_texto` - Dados de processamento de texto
- `id`: Chave primária
- `noticia_id`: Chave estrangeira para a tabela noticias
- `tipo_conteudo`: Tipo de conteúdo (TITULO, MANCHETE, CORPO)
- `texto_bruto`: Conteúdo de texto bruto (com índice FULLTEXT)
- `tokens_normalizados`: Tokens normalizados (com índice FULLTEXT)
- `tokens_stemming`: Tokens com stemming (LONGTEXT com índice FULLTEXT)
- `tokens_lemma`: Tokens lematizados (LONGTEXT com índice FULLTEXT)
- `outros_dados`: Metadados adicionais (JSON)
- `data_criacao`: Timestamp de criação
- `data_atualizacao`: Timestamp da última atualização

## Solução de Problemas

### Problemas de Conexão
- Verifique se o servidor MySQL está em execução
- Verifique as credenciais do banco de dados
- Certifique-se de que o usuário tem as permissões adequadas
- Verifique as configurações de firewall se conectando remotamente

### Problemas de Criação de Tabelas
- Certifique-se de que o usuário do banco de dados tem privilégios CREATE
- Verifique se existem tabelas com nomes conflitantes
- Verifique a compatibilidade da versão MySQL (8.0+ recomendado)

### Problemas de Salvamento de Dados
- Verifique URLs duplicadas (elas devem ser únicas)
- Verifique as restrições de chave estrangeira
- Verifique o formato dos dados JSON para campos de processamento de texto

## Testando a Conexão com o Banco de Dados

### Usando Python
Você pode testar a conexão com o banco de dados executando:

```python
from src.financial_analysis.persistance.database import initialize_database

if initialize_database():
    print("Conexão com o banco de dados bem-sucedida!")
else:
    print("Falha na conexão com o banco de dados!")
```

### Usando Linha de Comando

#### Windows (PowerShell/Prompt de Comando)
```cmd
mysql -u root -p -h localhost
# Digite a senha quando solicitado
```

#### Linux/macOS
```bash
mysql -u root -p -h localhost
# Digite a senha quando solicitado
```

### Verificar Criação do Banco de Dados
Uma vez conectado ao MySQL, você pode verificar se o banco de dados foi criado:

```sql
SHOW DATABASES;
USE investment_news;
SHOW TABLES;
```

Você deve ver o banco de dados `investment_news` com três tabelas: `tickers`, `noticias`, e `processamento_texto`.