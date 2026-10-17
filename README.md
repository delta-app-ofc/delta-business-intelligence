# Delta Business Intelligence

Scripts da arquitetura v1 do Delta: um job Mongo por coleção em cada camada, um job de extração SQL e um notebook por tabela Gold. A Gold BI corresponde à versão anterior de **24 KPIs**, com quatro bases BI e quatro bases da plataforma.

## Estrutura

```text
bronze/
  app/                  # Ingestão Mongo: alertas, chat, feedback, preferências e clima
  telemetry/            # Ingestão Mongo: status dos dispositivos
silver/
  app/                  # Transformações das cinco coleções do aplicativo
  telemetry/            # Transformações de consumo e status dos dispositivos
sql/
  07-ler-neon-glue.py   # Um job Glue Spark para os snapshots PostgreSQL
transfer/
  01-preparar-databricks.sql
  02-exportar-silver-glue.py
  03-enviar-files-api.py
  04-importar-silver-databricks.py
gold/
  00-gold-common.py
  01-consumption-window.py
  02-device-snapshot.py
  03-alert-case.py
  04-investment-scenario.py
  05-weather-hourly.py
  06-chat-session.py
  07-preferences-snapshot.py
  08-bill-monthly.py
  CONSULTAS.sql
```

Os scripts Bronze e Silver são executados no AWS Glue e armazenam Delta no S3. Cada arquivo preserva sua conexão, seu caminho de origem/destino e seus parâmetros originais. Clima e sessões têm duas saídas Silver no mesmo job, conforme os scripts de origem: `weather_daily`/`weather_hourly` e `chat_sessions`/`chat_messages`.

O script em `sql/` lê os snapshots SQL em um único job. `transfer/` contém a preparação do volume gerenciado, o exportador Glue Spark, o envio por Files API em Python Shell e o notebook Databricks que importa os snapshots nas tabelas Silver gerenciadas.

## Gold

| Notebook | Tabela final |
|---|---|
| `01-consumption-window` | `gold_platform.tb_consumption_window` |
| `02-device-snapshot` | `gold_platform.tb_device_snapshot` |
| `03-alert-case` | `gold_platform.tb_alert_case` |
| `04-investment-scenario` | `gold_platform.tb_investment_scenario` |
| `05-weather-hourly` | `gold_bi.tb_weather_hourly` |
| `06-chat-session` | `gold_bi.tb_chat_session` |
| `07-preferences-snapshot` | `gold_bi.tb_preferences_snapshot` |
| `08-bill-monthly` | `gold_bi.tb_bill_monthly` |

Importe os nove arquivos Python de `gold/` na mesma pasta do Workspace Databricks, preservando os nomes sem `.py`. Cada notebook de tabela carrega `00-gold-common` por `%run`; o apoio não é uma task separada. As oito bases alimentam os cálculos em `gold/CONSULTAS.sql`. O vínculo privado `gold_bi.tb_user_key` é um apoio técnico de pseudonimização.

## Ordem de execução

Bronze → Silver → extração SQL → exportação → envio → importação Silver no Databricks → Gold → consultas do dashboard. Os jobs preservam os controles de lote e workflow presentes nos arquivos. Use o mesmo `batch_id` e `cutoff_at` nos notebooks Gold e mantenha as fontes estáveis durante o lote. Chat, preferências e contas devem atualizar o vínculo de usuários em sequência.

## Origem e preservação

A referência de produção indicada pelo responsável é o conjunto local previamente montado, complementado pelos scripts originais das conversas. Os arquivos foram copiados sem alterar código, comentários, prefixos S3, nomes de conexões, regras ou formatação. A comparação de conteúdo é contra essas fontes; não houve leitura de uma conta AWS ativa nem execução de pipelines nesta importação.

- Bronze/Silver de status, clima e sessões: arquivos de `output/pipelines` do workspace de origem.
- Silver de consumo, alertas, feedback e preferências: blocos Python originais enviados pelo responsável na conversa “Definir KPIs Gold para BI”, em 12/10/2026.
- Bronze de alertas, feedback e preferências: blocos completos da conversa [Conectar MongoDB ao Databricks](https://chatgpt.com/c/6ab3af7d-916c-83e9-b039-1eb74b8860cc).
- Gold BI: arquivos preservados em `output/databricks/gold-previous/bi-v1-20261016`; apoio compartilhado e quatro notebooks de plataforma: `output/databricks/gold`.
- Extração SQL e transferência: arquivos de `output/databricks/files-api`.

**Recuperação pendente:** falta o script Bronze original de `consumption_summary`. Os arquivos locais e ZIPs consultados não contêm esse script; a mensagem da conversa original foi truncada pelo conector antes do fim do código. Ele não foi reconstruído por semelhança com outro job. Até sua recuperação, esta importação não representa o conjunto Bronze completo.

Este README concentra a descrição da estrutura. A documentação detalhada permanece destinada ao handbook.
