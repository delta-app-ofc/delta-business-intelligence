# Databricks notebook source
# MAGIC %md
# MAGIC # tb_investment_scenario
# MAGIC Atualiza somente esta base Gold.
# COMMAND ----------
# MAGIC %run ./00-gold-common
# COMMAND ----------
initialize_property_context()
unique(frames['c_tb_scenario_scope'], ['scenario_id', 'property_id', 'reference_month', 'scenario_version'],
       'Escopo de cenário duplicado.')
# Persist inputs only. Consumption, simulated savings and payback are consumer measures.
scope = (frames['c_tb_scenario_scope'].alias('c')
    .join(frames['p_tb_property'].alias('p'), F.col('c.property_id') == F.col('p.property_id'))
    .select('c.scenario_id', 'c.scenario_version', 'c.reference_month', 'p.organization_id')
    .distinct())
scenario = (frames['s_scenario'].alias('s')
    .join(scope.alias('c'), F.col('s.scenario_id') == F.col('c.scenario_id'), 'left')
    .select(F.col('s.scenario_id').alias('scenario_id'),
        F.coalesce(F.col('c.scenario_version'), F.lit('SQL_REFERENCE')).alias('scenario_version'),
        'c.reference_month', 'c.organization_id', 's.name',
        F.col('s.investment_value').cast('double').alias('investment_value'),
        's.reduction_pct', F.col('s.annual_savings_value').alias('annual_savings_reported'),
        F.col('s.payback_months').alias('payback_months_reported')))
save_table('gold_platform', 'tb_investment_scenario', scenario,
           ['scenario_id', 'scenario_version', 'reference_month', 'organization_id'])
