# quantilica-analytics

![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square) ![Python](https://img.shields.io/badge/python-3.12+-blue.svg?style=flat-square)

**Camada de processamento e padronização analítica da Quantilica.**

`quantilica-analytics` é a biblioteca especializada em leitura, validação e conversão de dados brutos para formatos analíticos de alto desempenho (Parquet), integrando-se nativamente com a infraestrutura de metadados do `quantilica-core`.

> **Note**
> For full documentation, please visit the official docs: **[https://docs.quantilica.com](https://docs.quantilica.com)**

## Objetivo

Desacoplar a lógica de processamento de dados (leitura, limpeza, conversão) do `quantilica-core`, mantendo o núcleo leve e focado exclusivamente em I/O (HTTP/FTP) e infraestrutura de metadados. Atua como uma ponte entre os arquivos brutos baixados pelos fetchers e os ativos analíticos prontos para consumo.

## Arquitetura

O pacote utiliza as melhores ferramentas modernas para processamento de dados de alto desempenho:
- **Polars**: Engine de processamento extremamente rápida escrita em Rust.
- **PyArrow**: Suporte robusto para escrita Parquet e schemas complexos.
- **quantilica-core**: Integração nativa com metadados de proveniência e sistemas de armazenamento.

## Módulos

### 1. `quantilica.analytics.reader`
Leitura de arquivos brutos, guiada ou não por um manifesto do core.
- `SmartReader.read()` / `.scan()`: `CSV` (padrão `;` e `latin-1`, com recuo de codificação), `Parquet`, `JSON` e `Excel` (`.xlsx`/`.xls`); aceita o caminho ou um `DownloadManifest`.
- `read_brazilian_csv()` e `read_csv_resilient()`: CSVs de dados abertos brasileiros (separador `;`, decimal `,`, codificação incerta, linhas quebradas); `normalize_brazilian_numbers()`.
- `quantilica.analytics.dbf`: `read_dbf()` e `read_dbc()` (dBase e DBC do DATASUS, sem dependências extras).
- `quantilica.analytics.fixed_width`: layouts de largura fixa (`layout_from_datasus`, `layout_from_sas_input`) e a consulta DuckDB que os lê (`fixed_width_sql`).

### 2. `quantilica.analytics.schema`
Contratos de dados para garantir consistência entre datasets.
- `DataContract` e `Field`: nomes de colunas, tipos e campos obrigatórios; `validate()` levanta erro se a fonte oficial alterar o layout do arquivo.
- `EraSchemaResolver`: renomeia colunas de eras históricas para o nome canônico, aplica os tipos esperados e preenche com nulos tipados o que falta nas eras antigas.

### 3. `quantilica.analytics.writer`
Escrita padronizada de Parquet.
- `to_parquet(data, output_path, manifest=..., contract=...)`: escrita atômica (arquivo temporário + `os.replace`), compressão `zstd` por padrão, validação opcional contra um `DataContract`, e a proveniência do `DownloadManifest` (como o `sha256` de origem) nos metadados do arquivo e num sidecar `<arquivo>.manifest.json`. Sem `manifest`, `source_path=` lê o sidecar do bruto.

### 4. Data Lake (`lake`, `lake_build`, `lake_audit`, `lake_publish`)
Motor do lake Hive em Parquet.
- `LakeWriter`, `SpecLakeConverter`/`convert_with_specs`, `scan_dataset`, `duckdb_view_sql` e `check_dataset`: escrita particionada com troca atômica, conversão por especificação e leitura por DuckDB/Polars.
- `lake_build`: construção retomável, um processo por `(fonte, dataset)`.
- `lake_audit`: auditoria de integridade, proveniência e completude do lake.
- `lake_publish`: marcador de atualização, manifesto do build e arquivo de estado.

### 5. `quantilica.analytics.geo`
Dimensões geográficas canônicas (`get_dim_geo`, `get_dim_uf`), conversão de código de município IBGE entre 6 e 7 dígitos e utilitários de geometria (WKB e bbox; o extra `geo` traz geopandas, pyogrio e pyproj).

## Benefícios

1. **Purismo nos Fetchers**: Um usuário que queira apenas baixar dados não precisa instalar dependências pesadas como Polars ou Arrow.
2. **Reuso de Código**: Centraliza a lógica complexa de lidar com encoding `latin-1` ou separadores `;` comuns em portais de dados brasileiros.
3. **Interoperabilidade**: Todos os arquivos gerados seguem padrões rígidos, permitindo consultas SQL (via DuckDB) em múltiplos datasets simultaneamente sem conflitos de tipo.

## Instalação

O `quantilica-analytics` **não está no PyPI**: é distribuído pelo índice estático próprio da Quantilica (`https://index.quantilica.com/simple/`). Adicione-o ao seu projeto:

```bash
uv add quantilica-analytics --index https://index.quantilica.com/simple/
```

As dependências (`quantilica-core`, `polars`, `pyarrow`, `duckdb`) vêm do PyPI. Para reprojeção e leitura de shapefile/GeoPackage, use o extra `geo`:

```bash
uv add "quantilica-analytics[geo]" --index https://index.quantilica.com/simple/
```

## Uso Rápido

```python
from quantilica.analytics.reader import SmartReader
from quantilica.analytics.writer import to_parquet
from quantilica.core.manifests import DownloadManifest

# Manifesto de um download feito pelo core (o sidecar fica ao lado do bruto)
manifest = DownloadManifest.read_json("data/raw/dataset.csv.manifest.json")

# Lê o arquivo apontado pelo manifesto (CSV: ';' e latin-1 por padrão)
df = SmartReader().read(manifest)

# Grava o Parquet com rastreabilidade total (proveniência injetada nos metadados)
to_parquet(df, "data/processed/dataset.parquet", manifest=manifest)
```

## Desenvolvimento

```bash
git clone https://github.com/Quantilica/quantilica-analytics.git
cd quantilica-analytics
uv sync --dev
uv run pytest
```

## Licença

MIT — veja [LICENSE](LICENSE).
