# quantilica-analytics

![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square) ![Python](https://img.shields.io/badge/python-3.12+-blue.svg?style=flat-square)

**Camada de processamento e padronização analítica da Quantilica.**

`quantilica-analytics` é a biblioteca especializada em leitura, validação e conversão de dados brutos para formatos analíticos de alto desempenho (Parquet), integrando-se nativamente com a infraestrutura de metadados do `quantilica-core`.

## Objetivo

Desacoplar a lógica de processamento de dados (leitura, limpeza, conversão) do `quantilica-core`, mantendo o núcleo leve e focado exclusivamente em I/O (HTTP/FTP) e infraestrutura de metadados. Atua como uma ponte entre os arquivos brutos baixados pelos fetchers e os ativos analíticos prontos para consumo.

## Arquitetura

O pacote utiliza as melhores ferramentas modernas para processamento de dados de alto desempenho:
- **Polars**: Engine de processamento extremamente rápida escrita em Rust.
- **PyArrow**: Suporte robusto para escrita Parquet e schemas complexos.
- **quantilica-core**: Integração nativa com metadados de proveniência e sistemas de armazenamento.

## Módulos

### 1. `quantilica.analytics.reader`
Abstração de leitura multi-formato integrada aos manifestos do core.
- Suporte a: `CSV`, `JSON`, `Excel`, `DBF` (comum no DATASUS).
- Integração com `quantilica.core.storage.LocalStorage`.
- Detecção automática de encoding e delimitadores.

### 2. `quantilica.analytics.schema`
Definição de contratos de dados para garantir consistência entre datasets.
- Classe `DataContract`: Define nomes de colunas padronizados, tipos e campos obrigatórios.
- Validação preventiva: Erro imediato se a fonte oficial alterar o layout do arquivo.

### 3. `quantilica.analytics.writer`
Conversão padronizada para formatos analíticos.
- `to_parquet()`: Escrita otimizada com compressão `zstd` e inclusão de metadados de proveniência (como o `sha256` original) no header do arquivo Parquet.
- Particionamento inteligente baseado nos metadados do dataset.

## Benefícios

1. **Purismo nos Fetchers**: Um usuário que queira apenas baixar dados não precisa instalar dependências pesadas como Polars ou Arrow.
2. **Reuso de Código**: Centraliza a lógica complexa de lidar com encoding `latin-1` ou separadores `;` comuns em portais de dados brasileiros.
3. **Interoperabilidade**: Todos os arquivos gerados seguem padrões rígidos, permitindo consultas SQL (via DuckDB) em múltiplos datasets simultaneamente sem conflitos de tipo.

## Instalação

O `quantilica-analytics` é publicado via GitHub. Adicione-o ao seu projeto:

```bash
uv add "quantilica-analytics @ git+https://github.com/Quantilica/quantilica-analytics.git"
```

## Uso Rápido

```python
from quantilica.analytics.writer import to_parquet
from quantilica.core.manifests import DownloadManifest

# Carrega um manifesto de um download realizado pelo core
manifest = DownloadManifest.read_json("data/raw/dataset.csv.manifest.json")

# Converte para Parquet com rastreabilidade total (proveniência injetada no header)
to_parquet(manifest, "data/processed/dataset.parquet")
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
