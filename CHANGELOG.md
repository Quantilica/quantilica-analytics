# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [Não lançado]

### Alterado
- `lake_build` passa a distinguir execuções. Ao terminar sem interrupção (com ou sem
  tarefas com erro), cada execução grava no fim do seu `.jsonl` uma linha de fechamento
  `{"run_id", "event": "run_complete", "finished_at", "tasks", "failed"}`; as linhas de
  tarefa não mudam. Ao iniciar: se a execução mais recente não tem o fechamento (foi
  interrompida), ela é retomada (mesmo `run_id` e arquivo, pulando as tarefas `ok`); se
  terminou, começa uma execução nova que roda todas as tarefas, e os conversores só
  reconvertem o que mudou no bruto (arquivos já no lake com o mesmo `sha256` são pulados).
  Antes, qualquer tarefa concluída em qualquer execução anterior era pulada para sempre,
  e chegar uma versão nova do bruto não gerava reconversão.
- `retry_errors=True` refaz só as tarefas com erro da execução mais recente (terminada ou
  não), mais as ainda não registradas dela se foi interrompida; numa execução terminada,
  abre uma execução nova só com essas tarefas. `force=True` abre sempre uma execução nova
  com tudo forçado. Um `run_id` explícito de execução já terminada levanta `LakeBuildError`;
  o de uma interrompida a retoma.
- `BuildSummary.resumed_run` informa se a execução retomou uma interrompida, e
  `BuildSummary.format()` passa a avisar. Arquivos `.jsonl` de versões anteriores, sem
  linha de fechamento, são tratados como execução interrompida (o mais recente é retomado).

## [0.8.0] - 2026-10-30

### Adicionado
- `lake_audit(lake_dir, input_dir=..., ledger=..., ...)` audita um lake e devolve um
  `AuditReport` (`ok`, `to_dict()`, `to_json()`, `to_text()`). Confere, sem ler os
  arquivos inteiros: os rodapés de todos os Parquets (incluindo os membros dos arquivos
  agrupados) e a integridade do último row group (lê só a coluna mais barata dele); o
  `sha256` de cada origem contra o bruto (manifesto ao lado do arquivo, ou o hash dele) e/ou
  contra um export do ledger em JSONL ou CSV (`caminho`, `sha256`); a cobertura; versões
  duplicadas; profundidade de partição uniforme por dataset; restos de escrita (`.tmp`); e o
  `check_dataset` de cada dataset. Junções e links de diretório não são contados duas
  vezes. Os rodapés são lidos em threads, o hash dos brutos sem manifesto também, e os
  brutos com manifesto nunca são relidos (`rehash=True` força). `report.timings` traz os
  segundos de cada etapa.
- A origem de um Parquet é casada por nome **e** `sha256`, não só pelo nome: o rodapé guarda
  o nome do arquivo sem a pasta, e uma fonte pode repetir o mesmo nome em várias pastas com
  conteúdos diferentes (`rfb-cnpj`). A proveniência está correta se algum arquivo bruto com
  aquele nome tiver o `sha256`; `sha256_divergente` só quando nenhum tiver, e o achado traz
  os `sha256` completos dos dois lados em `details` (no JSON). Origem com o `sha256` da
  versão vigente do mesmo arquivo, só com outro carimbo de data no nome, não é
  `origem_superada` e o arquivo vigente conta como coberto. `versao_duplicada` é o mesmo
  (nome, `sha256`) em mais de um Parquet ou membro, ou duas versões do mesmo arquivo
  (pasta, chave) no lake; nunca o mesmo nome em pastas diferentes, nem o mesmo arquivo que a
  fonte publicou em duas pastas.
- Com `input_dir` e `ledger` juntos, o ledger é a referência: uma origem que bate com o
  ledger, mas não com o arquivo do bruto local, não reprova; vira aviso de bruto local
  desatualizado (`counts["stale_raw_files"]`).
- Cobertura só nas fontes com conversor (`converters`, ou os entry points `quantilica.lake`
  quando `None`) ou com diretório no lake. Os arquivos das outras fontes só são contados
  (`counts["files_without_converter"]` e um aviso) e não reprovam. Arquivos sob pastas cujo
  nome começa com `_` (`_documentacao`, `_auxiliar`) contam como ignorados (`auxiliar`)
  quando o conversor não tem `inventory()` que os cubra; o `inventory()` prevalece.
  Arquivos vigentes sem Parquet, não ignorados, continuam sendo `sem_parquet`; os gêmeos de
  formato, os não tabulares, as exceções conhecidas e o que o `inventory()` declarar
  ignorado não reprovam.
- `footer_from_metadata` em `quantilica.analytics.lake`: o mesmo dicionário de `_footer`,
  a partir de um rodapé já aberto.
- `quantilica.analytics.lake_build.lake_build(input_dir, lake_dir, ...)`: constrói o lake
  a partir de uma árvore de brutos, com uma tarefa por `(fonte, dataset)`. Os datasets de
  cada fonte vêm de `converter.datasets()` (nunca do nome da pasta, que nem sempre é o id)
  e cada tarefa roda num processo próprio: `MemoryError`, queda do processo ou exceção de
  uma tarefa não derrubam as outras. Ao interromper (Ctrl+C, SIGTERM, SIGHUP, Ctrl+Break),
  todos os processos filhos e seus descendentes são encerrados.
- O andamento fica em `<lake>/.build/<run-id>.jsonl`, uma linha por tarefa concluída
  (fonte, dataset, início, fim, status `ok`/`erro`, contagens e mensagem). Uma nova
  execução pula o que já terminou sem erro; `retry_errors=True` refaz só as que falharam e
  `force=True` refaz tudo.
- Memória, threads e diretório temporário do DuckDB viram parâmetros (`memory`, `threads`,
  `scratch`); antes de converter, `lake_build` compara o espaço livre dos volumes com uma
  estimativa a partir do bruto pendente (`InsufficientDiskError`; `check_disk=False` ignora).
- `keep_awake()`: context manager que impede a suspensão do sistema (Windows, macOS e
  Linux, quando o sistema oferece o bloqueio); `lake_build` o usa por padrão.
- `BuildSummary` com o resumo por fonte (convertidos, pulados, erros, tarefas com erro) e
  `has_errors` para decidir o código de saída.

## [0.7.1] - 2026-10-30

### Corrigido
- `scan_dataset` (e o leitor Polars de `check_dataset`) lê datasets cujo esquema muda
  entre arquivos, como uma coluna que a fonte passou a publicar em anos posteriores.
  Antes, o Polars tomava o esquema do primeiro arquivo e recusava os demais ("extra
  column in file outside of expected schema"). Os arquivos passam a ser combinados pelo
  nome da coluna, com supertipos e nulos nas colunas ausentes, como o `union_by_name` do
  DuckDB; quando todos têm o mesmo esquema, a leitura continua num `scan_parquet` só.
- `check_dataset` conta as linhas no Polars por partição: num dataset com mais de 4,29
  bilhões de linhas, a contagem única estourava o inteiro de 32 bits do Polars.

## [0.7.0] - 2026-10-30

### Adicionado
- Arquivos agrupados no lake: `LakeWriter.write_member` grava um arquivo bruto no Parquet
  da partição (`<grupo>.parquet`), que reúne as linhas de vários brutos. Cada linha leva o
  `file_key` do bruto em `_origem`, as colunas de membro (como `uf`) entram como colunas
  literais, as linhas são ordenadas por elas e o rodapé guarda a proveniência de cada
  membro em `quantilica.members`. Regravar um membro troca só as suas linhas.
  `LakeWriter.is_current_member` pula o bruto que já está no grupo com o mesmo `sha256`.
- `compact_members(dataset_dir, member_keys)` converte um dataset particionado por essas
  chaves (por exemplo `uf=`) em arquivos agrupados, a partir dos Parquets existentes e
  sem reconverter os brutos. Quando há mais de uma versão do mesmo bruto, fica a mais
  nova. `workers` grava vários grupos ao mesmo tempo.

### Corrigido
- O `LakeWriter` grava o Parquet em disco (`fsync`) antes de publicá-lo por rename. Sem
  isso, uma queda de energia logo depois podia deixar o arquivo publicado truncado.

### Alterado
- Exige `quantilica-core>=0.14.1`, cujo `file_key` tira a data dos DBCs: `compact_members`
  depende disso para manter só a versão mais nova de cada bruto.

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
