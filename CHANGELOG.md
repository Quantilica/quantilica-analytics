# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [0.6.0] - 2026-10-29

### Adicionado
- `LakeWriter.write_duckdb` aceita `quote` e `escape` para fixar o dialeto do CSV.
  Sem eles (padrão), o DuckDB segue detectando o dialeto sozinho.
- `LakeWriter.is_current(src, partitions=...)` informa se o arquivo já está no lake com o
  mesmo `sha256`, para o conversor pular uma leitura cara antes de fazê-la.
- Variável `QUANTILICA_LAKE_SCRATCH`: quando definida, a extração de ZIP/7z e o
  temporário do DuckDB vão para esse diretório em vez do staging do dataset; o Parquet
  continua sendo montado ao lado do dataset e publicado por renomeação.
- `LakeWriter.write_duckdb` aceita `parallel=False` para ler com uma só thread, exigido
  pelo DuckDB quando há campos entre aspas com quebra de linha.

### Corrigido
- `write_duckdb` converte para UTF-8, antes da leitura, a entrada Latin-1 que contém
  bytes entre 0x80 e 0x9F. O decodificador `latin-1` do DuckDB rejeita esses bytes
  ("File is not latin-1 encoded"), embora sejam válidos em Latin-1.

## [0.5.0] - 2026-10-27

### Adicionado

- `lake`: novo módulo `quantilica.analytics.lake` para gravar e ler o lake Parquet particionado no estilo Hive (`<fonte>/<dataset>/<chave>=<valor>/<file_key>[-NNN].parquet`, com `_dataset.json`). Depende do módulo `quantilica.core.lake` (pin `quantilica-core>=0.13.0`).
  - `LakeWriter.write_duckdb()`: converte CSV/TXT, ZIP e 7z em streaming com DuckDB (qualquer encoding, com ou sem cabeçalho, `rename`). `required` recusa páginas de erro HTML salvas como CSV antes de gravar qualquer coisa. Colunas tipadas por um `DataContract` passam por `TRY_CAST`, e as falhas viram nulo contado em `quantilica.cast_nulls`.
  - `LakeWriter.write_polars()`: grava `DataFrame` ou `LazyFrame` (`sink_parquet` em streaming quando o plano permite) com as mesmas garantias.
  - Ambos gravam de forma atômica (`.tmp` + `os.replace`), seguram o lock do dataset, apagam partes `-NNN` órfãs de uma divisão anterior do mesmo `file_key`, recusam sobrescrever o arquivo de outro `file_key` e atualizam o `_dataset.json`. `max_part_bytes` divide a saída em partes.
  - Staging seguro de arquivos multi-partes: partes em `.tmp/` usam extensão `.parquet.tmp` durante a geração para isolar leituras concorrentes com globs `**/*.parquet` de capturarem arquivos intermediários.
  - Resolução ancorada de nomes de partes: chaves terminadas em hífen e números (ex.: `caged-001.csv`) são preservadas integralmente sem colisão na detecção de partes `-NNN`.
  - Remoção preventiva de colunas de partição: colunas coincidentes com as chaves de partição Hive são omitidas do payload físico do Parquet, prevenindo duplicação em disco.
  - Metadados `quantilica.*` no rodapé do Parquet: `origin_sha256`, `lake.layout_version`, `source_id`, `dataset_id`, `file_key`, `origin_file`, `converted_at`, `converter`, `contract.id`/`contract.version` e os campos do manifesto da origem.
  - Idempotência: o arquivo é pulado (`status="skipped"`) quando `origin_sha256`, a versão do layout e a versão do contrato batem com o Parquet existente; `force=True` reconverte.
  - `check_dataset()`: devolve a lista de desvios do contrato (descritor, diretórios `k=v`, nomes `<file_key>[-NNN].parquet`, metadados obrigatórios) e confere que DuckDB, Polars e PyArrow leem o mesmo número de linhas e os mesmos tipos de partição.
  - `scan_dataset()` (`LazyFrame` do Polars) e `duckdb_view_sql()` (`CREATE VIEW` com `hive_partitioning` e `hive_types`) leem o dataset com as colunas de partição nos tipos declarados no descritor, sem depender de inferência (`mes=09` continua `"09"`).

### Alterado

- `pyproject.toml`: `duckdb>=1.2.0` entra nas dependências.

## [0.4.0] - 2026-10-18

### Adicionado

- `writer`: `to_parquet()` aceita o parâmetro `source_path` para ler automaticamente o sidecar `<source_path>.manifest.json` via `read_manifest_sidecar` (`quantilica-core>=0.12.0`) quando `manifest` não for fornecido explicitamente.

### Alterado

- `pyproject.toml`: licença migrada para expressão SPDX PEP 639 (`license = "MIT"` + `license-files = ["LICENSE"]`), `hatchling>=1.27` e piso `quantilica-core>=0.12.0`.

## [0.3.0] - 2026-10-02
### Adicionado
- `normalize_brazilian_numbers()`: renormaliza colunas numéricas formatadas
  no padrão brasileiro (`1.234,56`) para `Float64` com cast seguro
  (`strict=False`), preservando colunas já numéricas e sentinelas (`-9999`,
  `N/D`, `NA`, `n.a.`, `n.d.`, `-`, `não disp`, `""`), além de sinal contábil
  entre parênteses/trailing (`(1.234,56)`, `1.234,56-`).
- `DEFAULT_NULL_SENTINELS`: constante canônica de sentinelas de null
  centralizada em `schema`, cruzando as listas de `reader` e `writer`.
- `read_brazilian_csv()` agora tenta UTF-8 primeiro e, em caso de erro de
  decodificação, faz fallback automático para Latin-1, com rewind do source
  consumível entre tentativas.
- `to_parquet()` com escrita atômica (arquivo temporário
  `<target>.tmp.parquet` no mesmo diretório seguido de `os.replace`) e
  persistência dos metadados `quantilica.*` derivados do manifest:
  sidecar JSON atômico em `<target>.manifest.json` + injeção no header
  Parquet.
### Alterado
- `DEFAULT_BR_NA` passa a ser alias de retrocompatibilidade de
  `DEFAULT_NULL_SENTINELS`, que agora também trata `n.a.`, `n.d.`, `-`,
  `não disp` e `""` como null por padrão.
- O grupo dev passa a instalar `pandas>=2.0`, exercitando o caminho
  `engine="pandas"` no suite de testes.

## [0.2.1] - 2026-08-31
### Corrigido
- Quitação de dívida de lint (E501/docstrings longas) herdada dos sweeps de
  documentação de 2026-08-14; nenhum comportamento alterado.

## [0.2.0] - 2026-06-04

Primeira entrada em formato Keep a Changelog; documenta o estado do pacote nesta
versão. Mudanças anteriores estão registradas no histórico de commits.

### Adicionado

- `quantilica.analytics.reader`: leitura multi-formato (CSV, JSON, Excel, DBF)
  integrada ao `LocalStorage` e aos manifestos do `quantilica-core`, com detecção
  automática de encoding e delimitadores.
- `quantilica.analytics.schema`: `DataContract` para validação preventiva de
  layout (colunas, tipos e campos obrigatórios).
- `quantilica.analytics.writer`: conversão para Parquet (`to_parquet()`) com
  compressão `zstd`, particionamento e metadados de proveniência (SHA-256) no
  header do arquivo.
