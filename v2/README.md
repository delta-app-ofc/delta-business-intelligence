# BI v2 — execução compacta no Glue

Quatro jobs, todos em sequência, preservando as sete saídas Gold, as nove tabelas Silver Mongo e os quatro snapshots SQL. Esta revisão fornece os scripts; não cria nem executa recursos na AWS ou no Databricks.

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
| `delta-v2-bronze` | `bronze/bronze.py` | Spark | `G.1X`, 2 workers | 10 min | 0 | 1 |
| `delta-v2-sql` | `sql/read_sql_database.py` | Spark | `G.1X`, 2 workers | 10 min | 0 | 1 |
| `delta-v2-silver` | `silver/silver.py` | Spark | `G.1X`, 2 workers | 10 min | 0 | 1 |
| `delta-v2-send` | `transfer/send_files.py` | Python Shell 3.9 | 0,0625 DPU | 10 min | 0 | 1 |

Esses são valores para configurar em **Job details**, não limites impostos pelo Python. Use a versão Spark/Delta compatível com o ambiente já usado pela v2. Nos três jobs Spark, mantenha `--datalake-formats=delta` e as configurações:

```text
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

Não configure o envio como Spark: ele usa apenas Python, boto3 e requests. As dependências Python Shell devem estar disponíveis no ambiente, por exemplo com `--additional-python-modules=requests`.

O workflow deve ter **Max concurrent runs = 1**, início sob demanda e apenas um job em cada trigger. Cada trigger condicional deve observar `SUCCEEDED` do job imediatamente anterior. Desative os gatilhos/agendamentos antigos: a concorrência de um workflow não impede outro workflow de escrever nos mesmos caminhos S3.

Teste cada etapa antes da primeira execução completa. A configuração reduz o pico de workers, mas não substitui o limite de consumo definido pelo laboratório.

## Bronze unificada

O job tem um único SparkContext, um único GlueContext e um único commit. Lê primeiro as cinco coleções app e depois as duas telemetry, usando as mesmas conexões:

- `delta-app-atlas-connection`, database `db_delta_app`;
- `delta-telemetry-atlas-connection`, database `db_delta_telemetry`.

Associe as duas conexões ao job, com rede compatível. Mantém os destinos `mongo-database/bronze/app/` e `mongo-database/bronze/telemetry/`, os casts, `_ingested_at`, MERGE por `_id`, evolução de schema e retry específico de mudança de schema. Tenta as demais coleções quando uma falha, mas encerra o job com erro ao final. Silver só pode iniciar após sucesso completo do fluxo anterior.

Coleção vazia mantém a regra anterior: não apaga uma Bronze existente. Portanto, vazio não representa limpeza de dados antigos. Confira a origem e os logs ao substituir cargas.

## SQL e Silver com exportação

O SQL permanece separado e deve concluir **antes da Silver**, no mesmo workflow. Mantenha `SQL_CHECK_ONLY=false` na carga real. `SQL_CHECK_ONLY=true` é apenas diagnóstico de acesso e não libera uma nova exportação.

O script `silver/silver.py` substitui os antigos `app_silver.py`, `telemetry_silver.py` e o job separado `export_files.py`. Preserva transformações, schemas, deduplicação e caminhos. Processa app, depois telemetry e, somente após ambos concluírem, exporta as 13 tabelas para Parquet.

Silver e SQL recebem `WORKFLOW_NAME` e `WORKFLOW_RUN_ID` do Glue. A Silver verifica `delta_sql_status=READY`, contrato SQL 2 e o mesmo workflow run antes de processar. Invalida uma exportação anterior antes de iniciar. Falha de transformação/exportação impede o commit de sucesso e o próximo trigger; os arquivos/tabelas já escritos não sofrem rollback automático.

A exportação agora ocorre na mesma sessão Spark da Silver. Mantém o manifest versão 4, o `batch_id` único, os snapshots Delta por versão e os quatro snapshots SQL. O manifest só é publicado depois de todas as exportações e o estado só muda para `delta_export_status=READY` ao final.

O bucket continua `delta-app-oficial-datalake`. Se a conta usar outro bucket, ajuste as constantes em Bronze, Silver, SQL e envio antes de publicar os scripts. As fontes devem permanecer estáveis durante o lote.

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
6. Confira o manifest com 13 tabelas, o Run ID aceito e, separadamente, o sucesso das tarefas Databricks e dos metadados de lote da Gold.

## Validação local

```text
python -m unittest discover -s v2/tests -p test_glue_pipeline.py -v
```

Os testes usam serviços simulados para conferir conexões/destinos, propagação de falhas, barreira de publicação do manifest e envio sem polling. Não validam conectividade Atlas/PostgreSQL, IAM, rede, gravações S3/Delta no Glue nem execuções reais no Databricks. As suítes existentes da Gold continuam separadas; seus contratos não foram alterados.
