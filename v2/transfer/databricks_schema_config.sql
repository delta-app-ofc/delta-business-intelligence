-- Exemplo com o catálogo "workspace". Confira SHOW CATALOGS e substitua
-- "workspace" nos scripts/notebook se seu catálogo tiver outro nome.
-- Volume gerenciado: usa o armazenamento padrão do Databricks.

-- Schema onde ficará o volume de chegada
CREATE SCHEMA IF NOT EXISTS workspace.delta;

CREATE VOLUME IF NOT EXISTS workspace.delta.delta_silver;

-- Schemas onde ficarão as tabelas importadas
CREATE SCHEMA IF NOT EXISTS workspace.silver_telemetry;
CREATE SCHEMA IF NOT EXISTS workspace.silver_app;
CREATE SCHEMA IF NOT EXISTS workspace.silver_sql;

DESCRIBE VOLUME workspace.delta.delta_silver;
