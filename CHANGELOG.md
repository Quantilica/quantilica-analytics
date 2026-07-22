# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

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
