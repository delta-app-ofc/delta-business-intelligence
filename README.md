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

Os arquivos deste repositório descrevem jobs e notebooks. Eles não criam os recursos na AWS ou no Databricks, não conectam automaticamente a um ambiente da equipe e não comprovam a execução de um pipeline.

## Versões dos pipelines

| Versão | Escopo documentado | Estado nesta cópia |
|---|---|---|
| [v1](v1/README.md) | Jobs por coleção e tabela; Gold BI anterior com 24 KPIs e bases de plataforma. | Versionada no repositório. |
| [v2](v2/README.md) | Execução compacta, com jobs unificados, sete saídas Gold, nove tabelas Mongo e quatro snapshots SQL. | Versionada no repositório. |
| [v3](v3/README.md) | Oito saídas Gold, dez tabelas Mongo e quatro snapshots SQL; inclui métricas de observabilidade de IA. | Presente neste checkout local, mas ainda não rastreada pelo Git. Um clone limpo não terá `v3/` até que a pasta seja incorporada ao repositório. |

As versões têm contratos e caminhos que evoluem juntos. Confira o `README.md` da versão escolhida antes de registrar ou atualizar jobs; evite executar gerações diferentes simultaneamente sobre os mesmos destinos.

## Fluxo de dados

```text
MongoDB ──> Bronze ──> Silver ──┐
                                ├──> transferência ──> Databricks ──> Gold ──> dashboards
PostgreSQL ──> snapshots SQL ──┘
```

O encadeamento exato, os parâmetros de lote, os manifests e as tabelas Gold variam por versão. Nas versões compactas, os jobs precisam compartilhar `batch_id` e horário de corte de acordo com a documentação correspondente. A transferência aceita o envio ao Databricks; a execução posterior do importador e da Gold deve ser acompanhada no ambiente Databricks.

## Estrutura do repositório

| Caminho | Conteúdo |
|---|---|
| `v1/` | Versão granular dos jobs Bronze, Silver, extração SQL, transferência e Gold. |
| `v2/` | Revisão compacta dos scripts e instruções de execução. |
| `v3/` | Revisão local mais recente, com consultas e observabilidade; ainda não rastreada nesta cópia. |
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
