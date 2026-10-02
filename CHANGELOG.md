# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

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
