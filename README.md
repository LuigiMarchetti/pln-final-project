# PLN
Nós vamos realizar análises de notícias voltadas ao mercado financeiro, realizando um web scraping dos sites de notícias e criando um algoritmo que dita a tendência de curto prazo do preço do ativo selecionado e resume os principais acontecimentos do ativo para análise (mudança de gestão, fusões, aquisições, novos produtos etc).

## Base de dados textuais:
Escolhemos 2 bases de dados distintas para não termos um viés único e não termos uma fonte única de informação caso um deles eventualmente saia fora do ar.
A questão de quantidade de dados não é um problema também, pois temos um número gigante de notícias relacionadas a ações brasileiras que podemos verificar.

Info Money: [https://www.infomoney.com.br/tudo-sobre/banco-do-brasil/](https://www.infomoney.com.br/tudo-sobre/banco-do-brasil/)
- Atualmente indiretamente pela XP Investimentos. Ela é a maior e mais importante corretora de investimentos do Brasil e tem mais de R$ 1 trilhão em ativos de clientes sob custódia, conectando investidores a uma ampla gama de produtos financeiros através das suas plataformas. Fonte: https://maisretorno.com/portal/xp-ultrapassa-r-1-trilhao-em-ativos-de-clientes-sob-sua-custodia
- Precisa clicar no "Carregar mais" e verificar as datas das notícias conforme queremos

Exame: [https://www.exame.com/noticias-sobre/banco-do-brasil](https://exame.com/noticias-sobre/banco-do-brasil/)
- O portal de notícias Exame é um importante veículo de comunicação brasileiro, focado em negócios, economia, política, tecnologia e carreira. A plataforma, que evoluiu de uma revista para um conglomerado de negócios, oferece notícias diárias, análises aprofundadas, entrevistas e conteúdo para assinantes através do portal e de serviços como a Exame Academy e Exame Research.
- É paginado, precisa varrer as páginas e verificar as datas das notícias conforme queremos



#### Além disso, ambos sites atualmente permitem que pesquisemos as notícias e usar a url de busca deles.

Info-money:
https://www.infomoney.com.br/robots.txt/
```txt
User-agent: *
Disallow: /wp-admin/
Disallow: /preview/
Disallow: /busca/
Disallow: /informe-publicitario/
```

Exame:
https://exame.com/robots.txt
```txt
User-agent: *
Disallow: /wp-admin/
Disallow: /preview/
Disallow: /busca/
Disallow: /informe-publicitario/
```
## Extras
- Newspaper3k: Além do scrapping com soup, surgiu uma outra alternativa: Utilziando o Newspaper3k, que é um pacote específico para sites de notícias, conseguimos fazer o parse do conteúdo de forma mais simples.
- TF-IDF + SequenceMatcher: Vetorização/Análise Semântica e Análise Sintática, para verificar a similaridade entre dois (ou mais, posteriormente) sites, assim podemos analisar a incidência de um conteúdo, comparando com vários sites sobre o mesmo tema.
