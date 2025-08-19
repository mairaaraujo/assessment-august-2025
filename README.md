# Assessment pipeline de dados - Vendas de Bebidas

Este repositório implementa um pipeline de dados para ingestão, transformação e modelagem de vendas de bebidas utilizando PySpark.

## Estrutura
- **src/pipeline/**: Scripts para ingestão, limpeza, transformação e modelagem dos dados.
- **files/input/**: Dados brutos (CSV).
- **files/output/**: Dados processados (Parquet), organizados por camadas (landing, silver, etc).
- **notebooks/**: Notebooks para exploração e validação dos dados.

## Modelagem
O pipeline segue o modelo dimensional (star schema), com tabelas fato e dimensões para produto, embalagem, canal e data.

## Como executar
1. Configure os caminhos dos arquivos no `.env`.
2. Execute os scripts em `src/pipeline/` para processar os dados.
3. Os resultados serão salvos em `files/output/`.

## Requisitos
- Python 3.8+
- PySpark
- python-dotenv

