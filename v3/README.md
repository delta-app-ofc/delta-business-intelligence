# BI v3 — execução compacta no Glue

Quatro jobs, todos em sequência, preservando as oito saídas Gold, as dez tabelas Silver Mongo e os quatro snapshots SQL. Esta revisão fornece os scripts; não cria nem executa recursos na AWS ou no Databricks.

```text
Bronze (app + telemetry)
  → SQL (quatro snapshots)
  → Silver (app + telemetry + exportação)
  → envio por Python Shell (upload + disparo, sem espera)
      → Databricks: import_silver → gold_bi
```

## Scripts e configurações no Glue

| Job sugerido | Script | Tipo | Capacidade inicial | Timeout | Retries | Concorrência |
|---|---|---|---|---|---|---|
| `delta-v3-bronze` | `bronze/bronze.py` | Spark | `G.1X`, 2 workers | 10 min | 0 | 1 |
| `delta-v3-sql` | `sql/read_sql_database.py` | Spark | `G.1X`, 2 workers | 10 min | 0 | 1 |
| `delta-v3-silver` | `silver/silver.py` | Spark | `G.1X`, 2 workers | 10 min | 0 | 1 |
| `delta-v3-send` | `transfer/send_files.py` | Python Shell 3.9 | 0,0625 DPU | 10 min | 0 | 1 |

Esses são valores para configurar em **Job details**, não limites impostos pelo Python. Use a versão Spark/Delta compatível com o ambiente já usado pela v2. Nos três jobs Spark, mantenha `--datalake-formats=delta` e as configurações:

```text
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

Não configure o envio como Spark: ele usa apenas Python, boto3 e requests. As dependências Python Shell devem estar disponíveis no ambiente, por exemplo com `--additional-python-modules=requests`.

O workflow deve ter **Max concurrent runs = 1**, início sob demanda e apenas um job em cada trigger. Cada trigger condicional deve observar `SUCCEEDED` do job imediatamente anterior. Desative os gatilhos/agendamentos antigos: a concorrência de um workflow não impede outro workflow de escrever nos mesmos caminhos S3.

Teste cada etapa antes da primeira execução completa. A configuração reduz o pico de workers, mas não substitui o limite de consumo definido pelo laboratório.

## Bronze unificada

O job tem um único SparkContext, um único GlueContext e um único commit. Lê primeiro as seis coleções app e depois as duas telemetry, usando as mesmas conexões:

- `delta-app-atlas-connection`, database `db_delta_app`;
- `delta-telemetry-atlas-connection`, database `db_delta_telemetry`.

Associe as duas conexões ao job, com rede compatível. Mantém os destinos `mongo-database/bronze/app/` e `mongo-database/bronze/telemetry/`, os casts, `_ingested_at`, MERGE por `_id`, evolução de schema e retry específico de mudança de schema. Tenta as demais coleções quando uma falha, mas encerra o job com erro ao final. Silver só pode iniciar após sucesso completo do fluxo anterior.

Coleção vazia mantém a regra anterior: não apaga uma Bronze existente. Portanto, vazio não representa limpeza de dados antigos. Confira a origem e os logs ao substituir cargas.

## SQL e Silver com exportação

O SQL permanece separado e deve concluir **antes da Silver**, no mesmo workflow. Mantenha `SQL_CHECK_ONLY=false` na carga real. `SQL_CHECK_ONLY=true` é apenas diagnóstico de acesso e não libera uma nova exportação.

O script `silver/silver.py` substitui os antigos `app_silver.py`, `telemetry_silver.py` e o job separado `export_files.py`. Preserva transformações, schemas, deduplicação e caminhos. Processa app, depois telemetry e, somente após ambos concluírem, exporta as 14 tabelas para Parquet.

Silver e SQL recebem `WORKFLOW_NAME` e `WORKFLOW_RUN_ID` do Glue. A Silver verifica `delta_sql_status=READY`, contrato SQL 2 e o mesmo workflow run antes de processar. Invalida uma exportação anterior antes de iniciar. Falha de transformação/exportação impede o commit de sucesso e o próximo trigger; os arquivos/tabelas já escritos não sofrem rollback automático.

A exportação agora ocorre na mesma sessão Spark da Silver. Mantém o manifest versão 5, o `batch_id` único, os snapshots Delta por versão e os quatro snapshots SQL. O manifest só é publicado depois de todas as exportações e o estado só muda para `delta_export_status=READY` ao final.

O bucket continua `delta-ofc-datalake`. Se a conta usar outro bucket, ajuste as constantes em Bronze, Silver, SQL e envio antes de publicar os scripts. As fontes devem permanecer estáveis durante o lote.

## Envio assíncrono ao Databricks

Parâmetros obrigatórios do envio:

```text
--DATABRICKS_SECRET_ID <segredo-com-host-e-token>
--DATABRICKS_JOB_ID <id-do-job-de-importacao-e-gold>
```

O segredo continua sendo JSON com `host` HTTPS base e `token`, no Secrets Manager. O job necessita de acesso ao segredo, aos arquivos S3 e às propriedades da execução Glue. Não coloque credenciais no repositório.

Parâmetros opcionais preservados:

```text
--TARGET_CATALOG workspace
--VOLUME_PATH /Volumes/workspace/delta/delta_silver
--CHECK_ONLY false
```

`WORKFLOW_NAME` e `WORKFLOW_RUN_ID` são fornecidos pelo workflow. `CHECK_ONLY=true` continua permitindo conferir segredo, job e volume isoladamente, sem enviar lote nem disparar execução. Remova os antigos parâmetros `WAIT_TIMEOUT_SECONDS` e `POLL_SECONDS`: não há mais polling de `jobs/runs/get`.

O envio valida o manifest READY do mesmo workflow, envia os Parquets e publica o manifest no volume por último. Só então chama `jobs/run-now`, com o mesmo token de idempotência derivado de workflow, run, lote e Job ID. Após receber um `run_id` válido:

- registra `delta_databricks_status=SUBMITTED` e `delta_databricks_run_id` nas propriedades do workflow;
- imprime Job ID, Run ID e lote nos logs;
- encerra o Python Shell imediatamente, sem aguardar importação/Gold.

**SUCCEEDED no Glue agora significa arquivos enviados e disparo aceito.** A conclusão ou falha da importação/Gold deve ser acompanhada no Databricks. `SUBMITTED` não muda automaticamente para `SUCCESS`/`FAILED` nas propriedades Glue. Falha de upload ou de submissão continua falhando o envio. Se o registro das propriedades falhar depois da API aceitar a execução, localize o Run ID no log antes de tentar novamente; isso não comprova falha do Databricks.

## Job Databricks e migração

1. Preserve a tarefa inicial `import_silver`, usando `transfer/import_silver_databricks.py`.
2. Encadeie a tarefa `gold_bi`, com `gold/gold_bi.py`, ao sucesso de `import_silver`.
3. Cadastre os parâmetros de job `catalog`, `volume_path` e `batch_id` e repasse-os aos notebooks aplicáveis. A Gold precisa do mesmo `catalog` e `batch_id` importados.
4. Mantenha **Max concurrent runs = 1**. O job principal precisa aceitar o disparo pela API; o agendamento do pipeline Databricks de fallback pode continuar pausado.
5. Suba os novos scripts e atualize os triggers para a sequência deste documento. Os cinco scripts antigos de Bronze/Silver/exportação foram substituídos; não mantenha seus jobs disparando em paralelo.
6. Confira o manifest com 14 tabelas, o Run ID aceito e, separadamente, o sucesso das tarefas Databricks e dos metadados de lote da Gold.

## Validação local

```text
python -m unittest discover -s v3/tests -p test_glue_pipeline.py -v
```

Os testes usam serviços simulados para conferir conexões/destinos, propagação de falhas, barreira de publicação do manifest e envio sem polling. Não validam conectividade Atlas/PostgreSQL, IAM, rede, gravações S3/Delta no Glue nem execuções reais no Databricks. As suítes existentes da Gold continuam separadas; seus contratos não foram alterados.

## Metricas de IA em observability_requests

A Bronze inclui observability_requests do banco de aplicacao. A Silver preserva request_id, created_at, mode, outcome, resolved, total_ms e agents_ms (mapa de nomes para duracoes). Os demais campos de custos/tokens continuam na Bronze e nao integram os KPIs atuais.

A Gold inclui os modos chat e judge. tb_ai_request tem uma linha por request_id; tb_agent_call tem uma linha por request_id e agente presente em agents_ms. Requisicoes sem agentes continuam na contagem global. Duracoes sao acumuladas por agente/requisicao, nao por chamada individual de modelo. outcome permanece global; nao e convertido em codigo HTTP.

BI14 conta requisicoes por outcome. BI15 mede participacoes acima de 5000 ms. BI16 mostra sua distribuicao. BI17 mede a proporcao de requisicoes com outcome=error entre as participacoes de cada agente, sem atribuir falha individual ao agente; partial e rejected permanecem no denominador. BI18 calcula P90 por dia de Sao Paulo e agente. Os dois modos sao agregados nesses indicadores; requisições distintas sao contadas separadamente.

Publique Bronze, Silver, envio, importador e Gold juntos antes da nova carga: manifesto v5, 10 tabelas Mongo + 4 SQL. O SQL permanece no contrato 2. Depois do sucesso da Gold, atualize os datasets BI14-BI18 usando gold/CONSULTAS-DASHBOARD.sql e os titulos/legendas do dashboard. A tabela tb_agent_call muda de schema; consultas antigas com session_id, message_index ou api_status_code precisam ser substituidas.

Essa migracao nao corrige coordenadas invalidas dos imoveis, que continuam sujeitas a validacao da Gold. Nao ha preenchimento ficticio de agente, status HTTP ou duracao.

Esta versao e independente da v2 preservada no repositorio. As consultas do dashboard ficam somente em gold/CONSULTAS-DASHBOARD.sql. O contrato SQL 2 e os caminhos S3 continuam os mesmos; nao execute as duas versoes simultaneamente sobre esses destinos.
