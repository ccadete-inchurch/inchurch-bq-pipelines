# Inchurch Pipeline

API de pipelines ETL que extrai dados da API do Superlogica e carrega no PostgreSQL. Desenvolvida em Flask e hospedada no Google App Engine, expõe endpoints HTTP que disparam pipelines de forma assíncrona via ThreadPoolExecutor.

---

## Tecnologias

| Tecnologia | Versão | Uso |
|---|---|---|
| Python | 3.12 | Linguagem principal |
| Flask | 3.1.2 | Framework HTTP |
| Gunicorn | 23.0.0 | WSGI server (App Engine) |
| Pandas | 2.3.2 | Transformação de dados |
| SQLAlchemy | 2.0.43 | ORM / conexão PostgreSQL |
| pg8000 | — | Driver PostgreSQL puro Python |
| Cloud SQL Python Connector | — | Conexão segura via Unix socket |
| Google App Engine | Standard F1 | Hospedagem da API |
| Google Cloud SQL | PostgreSQL | Banco de dados destino |
| N8N | — | Orquestração e monitoramento dos pipelines |

---

## Arquitetura

```
HTTP Request (N8N / manual)
        │
        ▼
   Flask (app.py)
        │
        ▼
 ThreadPoolExecutor  ──► fire-and-forget (retorna 200 imediatamente)
        │
        ▼
  Pipeline (pipelines/)
        │
   ┌────┴────┐
   │         │
Superlogica  PostgreSQL
   API       (Cloud SQL)
```

### Fluxo de execução

1. Requisição HTTP chega ao endpoint
2. Flask retorna `200 OK` imediatamente
3. Pipeline é submetido ao `ThreadPoolExecutor` (max 3 workers)
4. Pipeline extrai dados da API Superlogica (paginado, com retry)
5. Dados são transformados com Pandas
6. Carga no PostgreSQL via upsert ou replace
7. Resultado registrado em `splgc_validacoes` para monitoramento pelo N8N

---

## Estrutura do projeto

```
.
├── app.py                              # Flask app + factory de rotas
├── config.py                           # Configuração via variáveis de ambiente
├── route_registry.py                   # Registro de todos os endpoints
├── requirements.txt
├── .env.example                        # Template de variáveis de ambiente
├── pipelines/
│   ├── pipeline_base.py               # BasePipeline: extração, carga, validação
│   ├── base_classes.py                # Classes base por entidade (processamento)
│   ├── pipeline_factory.py            # Factory para main functions simples
│   ├── acordos.py
│   ├── base_mrr.py
│   ├── clientes_A6.py
│   ├── clientes_inchurch.py
│   ├── cobrancas_comp_A6.py
│   ├── cobrancas_comp_A6_hist.py
│   ├── cobrancas_comp_inchurch.py
│   ├── cobrancas_comp_inchurch_hist.py
│   ├── cobrancas_liq_A6.py
│   ├── cobrancas_liq_inchurch.py
│   ├── despesas.py
│   ├── grupo.py
│   ├── produto.py
│   └── produtos_mrr.py
└── utils/
    └── threadpool_manager.py          # Gerenciamento do ThreadPoolExecutor
```

---

## Endpoints

Todos os endpoints disparam o pipeline de forma assíncrona e retornam imediatamente.

### Cobranças

| Método | Endpoint | Descrição |
|---|---|---|
| GET | `/cobrancas/comp_inchurch/<year>/<month>` | Cobranças por competência Inchurch. `month=0` executa loop bimestral do ano |
| GET | `/cobrancas/comp_inchurch_hist/<year>/<month>` | Cobranças por competência Inchurch histórico |
| GET | `/cobrancas/liq_inchurch/<year>/<month>` | Cobranças por liquidação Inchurch. `month=0` executa loop mensal do ano |
| GET | `/cobrancas/comp_a6` | Cobranças por competência A6 (2025–2026) |
| GET | `/cobrancas/comp_a6_hist` | Cobranças por competência A6 histórico (2021–2024) |
| GET | `/cobrancas/liq_a6` | Cobranças por liquidação A6 |

### Clientes

| Método | Endpoint | Descrição |
|---|---|---|
| GET | `/clientes/inchurch` | Clientes Inchurch |
| GET | `/clientes/a6` | Clientes A6 |

### Financeiro

| Método | Endpoint | Descrição |
|---|---|---|
| GET | `/despesas/<year>` | Despesas do ano informado |
| GET | `/acordos` | Acordos |

### MRR e Produtos

| Método | Endpoint | Descrição |
|---|---|---|
| GET | `/mrr/<year>` | Tabela MRR (recorrências ativas desde 01/01/year) |
| GET | `/produtos_mrr` | Produtos base MRR |
| GET | `/produto` | Produtos por cliente |
| GET | `/grupo` | Grupos por cliente |

---

## Tabelas PostgreSQL

| Tabela | Pipeline | Modo |
|---|---|---|
| `splgc-cobrancas_competencia-all` | comp_inchurch | upsert |
| `splgc-cobrancas_competencia-hist` | comp_inchurch_hist, comp_a6_hist | upsert |
| `splgc-cobrancas_competencia-A6-2025-2026` | comp_a6 | replace |
| `splgc-cobrancas_liquidacao-all` | liq_inchurch, liq_a6 | upsert |
| `splgc-clientes-inchurch` | clientes_inchurch | upsert |
| `splgc-clientes-a6` | clientes_a6 | upsert |
| `splgc-despesas-<year>` | despesas | replace |
| `splgc-tabela_mrr` | mrr | upsert |
| `splgc-produtos_base_mrr` | produtos_mrr | replace |
| `splgc-produto` | produto | replace |
| `splgc-grupo` | grupo | replace |
| `splgc-acordos` | acordos | upsert |
| `splgc_validacoes` | todos | insert (log de execução) |

---

## Configuração

Todas as credenciais são lidas de variáveis de ambiente. Copie `.env.example` para `.env` e preencha:

```bash
cp .env.example .env
```

| Variável | Descrição |
|---|---|
| `INCHURCH_TOKEN` | Token de acesso Superlogica Inchurch |
| `A6_TOKEN` | Token de acesso Superlogica A6 |
| `SUPERLOGICA_APP_TOKEN` | App token Superlogica |
| `SUPERLOGICA_API_URL` | URL base da API Superlogica |
| `DB_CONNECTION_STRING` | String de conexão PostgreSQL |
| `API_TIMEOUT` | Timeout das requisições em segundos (padrão: 180) |
| `API_MAX_RETRIES` | Tentativas por página (padrão: 3) |
| `API_RATE_LIMIT_SLEEP` | Pausa entre páginas em segundos (padrão: 0.2) |
| `DB_BATCH_SIZE` | Tamanho do batch de inserção (padrão: 5000) |
| `THREAD_POOL_MAX_WORKERS` | Máximo de pipelines simultâneos (padrão: 3) |

No App Engine, as variáveis são definidas no `app.yaml` (não versionado).

---

## Deploy

```bash
gcloud app deploy
```

O `app.yaml` define o runtime, timeout do Gunicorn (1800s) e as variáveis de ambiente de produção.

---

## Monitoramento

Cada pipeline registra sua execução na tabela `splgc_validacoes` com:
- `script`: nome do pipeline e período executado
- `mensagem`: resultado detalhado com contagem de registros
- `sucesso`: boolean
- `dt_update`: data da execução

O N8N consulta essa tabela diariamente para verificar quais pipelines rodaram e envia resumo para o Google Chat.

---

## Desenvolvimento local

```bash
python -m venv venv
venv\Scripts\activate      # Windows
pip install -r requirements.txt
# configure o .env
python app.py
```
