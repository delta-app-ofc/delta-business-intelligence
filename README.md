# Delta · Business Intelligence

![Python 3.9 no CI de sintaxe](https://img.shields.io/badge/CI%20de%20sintaxe-Python%203.9-3776AB?logo=python&logoColor=white)
![Python 3.12 nos testes offline](https://img.shields.io/badge/Testes%20offline-Python%203.12-3776AB?logo=python&logoColor=white)
![AWS Glue](https://img.shields.io/badge/Pipelines-AWS%20Glue%20%2B%20Spark-FF9900?logo=amazonaws&logoColor=white)
![Databricks](https://img.shields.io/badge/Analytics-Databricks%20Delta%20Lake-EF3E2F?logo=databricks&logoColor=white)

Este repositório reúne os pipelines analíticos do Projeto Delta. Os jobs organizam dados de hidrômetros e aplicações nas camadas Bronze, Silver e Gold e preparam tabelas para indicadores e dashboards.

**Tags:** `business-intelligence` · `etl` · `aws-glue` · `spark` · `databricks` · `delta-lake` · `bronze-silver-gold` · `iot` · `mongodb` · `postgresql`

## Sumário

- [Visão geral](#visão-geral)
- [Versões dos pipelines](#versões-dos-pipelines)
- [Fluxo de dados](#fluxo-de-dados)
- [Estrutura do repositório](#estrutura-do-repositório)
- [Configuração e execução](#configuração-e-execução)
- [Testes e CI](#testes-e-ci)
- [Repositórios relacionados](#repositórios-relacionados)

## Visão geral

| Componente | Papel |
|---|---|
| Bronze | Ingestão dos dados de origem MongoDB nas áreas do data lake. |
| Extração SQL | Leitura de snapshots relacionais usados pelos processos analíticos. |
| Silver | Padronização e transformação dos dados de origem. |
| Transferência | Exportação e envio de lotes ao ambiente Databricks. |
| Gold | Tabelas analíticas de plataforma e de Business Intelligence usadas por consultas e dashboards. |

## Versões dos pipelines

| Versão | Escopo |
|---|---|
| [v1](v1/README.md) | Jobs por coleção e tabela; camada Gold de BI com 24 KPIs e bases de plataforma. |
| [v2](v2/README.md) | Jobs unificados, sete saídas Gold, nove tabelas Mongo e quatro snapshots SQL. |

As pastas `v1/` e `v2/` contêm gerações distintas dos pipelines. Consulte o README da versão para configurar os jobs, os parâmetros e os destinos correspondentes.

## Fluxo de dados

```text
MongoDB ──> Bronze ──> Silver ──┐
                                ├──> transferência ──> Databricks ──> Gold ──> dashboards
PostgreSQL ──> snapshots SQL ──┘
```

Cada versão documenta os parâmetros de lote, manifests e tabelas Gold. Nos pipelines compactos, mantenha `batch_id` e horário de corte consistentes entre jobs; a etapa de transferência entrega os lotes ao Databricks para importação e geração da camada Gold.

## Estrutura do repositório

| Caminho | Conteúdo |
|---|---|
| `v1/` | Versão granular dos jobs Bronze, Silver, extração SQL, transferência e Gold. |
| `v2/` | Versão compacta dos scripts e instruções de execução. |
| `tests/` | Testes offline do contrato de envio de lotes. |
| `.github/workflows/test.yml` | Verificação de sintaxe Python 3.9 para a v1 em Pull Requests e execução manual. |
| `.github/workflows/sonar.yml` | Testes offline e análise de qualidade com Python 3.12. |
| `.github/workflows/trigger_actions.yml` | Chamada às automações compartilhadas de Pull Request da organização. |
| `requirements-dev.txt` | Dependências para os testes offline. |
| `pipelines-v2-fallback/` e `relatorio-gerencial/` | Diretórios auxiliares com materiais das versões anteriores. |

## Configuração e execução

Escolha a versão do pipeline e siga o README correspondente para configurar conexões, caminhos S3, parâmetros, permissões e tarefas do AWS Glue e Databricks.

Para instalar as dependências de desenvolvimento e executar a validação local da v1, use Python:

```bash
python -m pip install -r requirements-dev.txt
python -m compileall -q v1
python -m pytest -q tests
```

## Testes e CI

`test.yml` compila os scripts da v1 com Python 3.9. O workflow `sonar.yml` usa Python 3.12, compila a v1 e executa a suíte offline, que cobre o contrato de envio de lotes com respostas sintéticas e publica relatórios de teste e cobertura.

## Repositórios relacionados

- [delta-handbook — processo e indicadores de BI](https://github.com/delta-app-ofc/delta-handbook/tree/main/DADOS/BI).
- [delta-sql-database](https://github.com/delta-app-ofc/delta-sql-database) — origem relacional e scripts PostgreSQL.
- [delta-mongo-database](https://github.com/delta-app-ofc/delta-mongo-database) — coleções usadas pelos fluxos MongoDB.
