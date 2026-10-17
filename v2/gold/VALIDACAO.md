# Validação da revisão de governança — 17/10/2026

- **19 testes em Spark 4.0.1/Java 21 local passaram**, executando as transformações reais e resolvendo os 13 datasets mais as opções de região. Cobrem conservação de volume, sobreposições, cobertura incompleta, normalização das cidades, conflitos entre nomes/IDs de região, clima ausente/conflitante, coordenadas obrigatórias, schema enxuto de IA, instrumentação, exclusão de juiz/roteador da latência, metas elegíveis, tarifas e disponibilidade.
- **3 testes de fluxo com fronteiras Spark/Delta simuladas passaram**: as sete validações ocorrem antes da primeira escrita; qualidade inválida na última saída impede todas as escritas analíticas; falha de armazenamento é propagada e não é reportada como sucesso.
- A primeira execução da revisão identificou um filtro de região ainda presente em BI05. Foi corrigido no SQL e nas cópias do prompt; a suíte completa passou na execução final.
- A execução da JVM exigiu acesso fora do sandbox do Windows e paths curtos para temporários. O encerramento do Spark produziu avisos de finalização de processos no Windows, após a suíte concluir com OK e exit code 0.

Comandos a partir da raiz `Repositorios`:

```powershell
python delta-business-intelligence/v2/tests/test_gold_governance.py
python delta-business-intelligence/v2/tests/test_publication.py
```

O teste Spark aceita `DELTA_SPARK_DEPS`, `DELTA_TEST_TMP` e `JAVA_HOME`; no workspace usa as dependências de teste existentes. As fixtures são locais e não contêm dados pessoais reais. Os testes de publicação usam fakes; não são escrita Delta real.

As cópias de transformações do pacote de notebooks separados e da migração sem S3 foram alinhadas à mesma fonte. Python/JSON e ZIPs são conferidos localmente. Não foram executados: consultas de diagnóstico no Databricks, leitura de Atlas/Neon, jobs Glue, escrita Delta/Unity Catalog ou edição/publicação de dashboard. A contagem de três horas é comprovada no seed versionado, não no ambiente remoto.
