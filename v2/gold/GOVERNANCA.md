# Revisão da governança BI — 17/10/2026

Contrato de qualidade 3, mantendo os 13 KPIs e as sete tabelas analíticas. A referência executável é `gold_bi.py`; consultas e prompt devem acompanhar esta revisão. Não implica que o Databricks já recebeu estes arquivos.

## Achados no código anterior

- `upper(trim(city))` não unificava `São Paulo` com `Sao Paulo`. O join de consumo com clima podia perder observações por acento ou espaços.
- `region_name` atravessava as transformações sem padronização ou validação entre tarifas e imóveis.
- `tb_agent_call` carregava UUID de usuário e região inferida dos imóveis. Usuários sem vínculo SQL ou com mais de uma região ficavam com região nula; isso não localiza a chamada e não é necessário a BI05/BI09.
- A meta pertence ao usuário e pode abranger vários imóveis. Atribuí-la a uma região única também era uma inferência. Os dias inelegíveis eram publicados com medidas nulas.
- Temperaturas, leituras e disponibilidade incompletas podiam ser silenciosamente filtradas. O mapa omitia imóveis com coordenadas ausentes, embora o resultado fosse apresentado como consumo por imóvel.
- A validação de gravação cobria principalmente duplicidade e chaves nulas, sem contrato completo de colunas/medidas ou diagnóstico da cobertura.
- Os prints mostram uma montagem de dashboard incompatível com os KPIs locais. Um nome de agente disponível na tabela não configura automaticamente a dimensão do visual.

## Endereços e regiões

Cidade é padronizada em maiúsculas, sem acentos, sem espaços repetidos e sem marcas combinantes Unicode. Assim, `São Paulo`, `SAO PAULO` e `Sao  Paulo` resultam em `SAO PAULO` tanto no contexto SQL quanto no clima Mongo. Estado SP também aceita o nome por extenso. Nomes de região usam a mesma normalização e `_` entre palavras; IDs SQL são preservados.

Um ID de região só pode ter um nome canônico; um nome só pode identificar um ID. A validação cruza imóveis e tarifas e falha se houver contradição. A correção de nomes não altera a identidade cadastral.

**`GRANDE_SP` não é uma variante de cidade `SAO PAULO`.** Região cadastral pode agrupar cidades. Tarifas podem existir para mais regiões/classes do que aquelas com leituras de consumo ou alertas. Isso é diferença de cobertura, não motivo para colapsar tudo em `GRANDE_SP`, criar endereços ou espalhar pontos. `classification_id` distingue categorias tarifárias dentro de RESIDENCIAL/COMERCIAL.

Coordenadas de imóveis SP são obrigatórias, finitas, dentro dos limites geográficos e diferentes de `(0,0)`. Ausência/invalidez bloqueia as saídas dependentes desse contexto até corrigir `tb_address`. Limites numéricos não comprovam que a coordenada corresponde ao endereço; essa conferência exige referência geográfica confiável, ausente no contrato atual.

## Fatos de IA e metas

`tb_agent_call` publica somente `session_id`, `message_index`, `occurred_at`, `agent_name`, `response_time_ms` e `api_status_code`. Sessão/índice identificam a observação; nome agrupa os especialistas. Não publica UUID de usuário, região ou endereço inferidos. Os nomes são padronizados (`LeakAgent`/`leak_agent` → `leak`), preservando identificadores numéricos.

Nome do agente, instante e HTTP são obrigatórios. Especialista com sucesso precisa de duração finita e não negativa, informada pelo backend. Falha HTTP pode não ter duração. Juiz/roteador conservam seu nome para contagem de falhas, mas não recebem duração analítica e não entram no boxplot. Esses são os únicos motivos admitidos para duração nula; nunca preencher com zero. Chamadas sem instrumentação obrigatória bloqueiam a tabela, sem inventar nomes.

`tb_target_compliance_daily` publica somente usuários/dias completos com meta diária positiva. Os campos de região foram removidos: meta é global do usuário. Dias sem cobertura ou meta não entram no fato nem no denominador; sua quantidade é registrada no log. A média continua sendo dos percentuais individuais, e a referência continua sendo a meta atual.

**BI05, BI09 e BI12 respondem somente ao período.** O dashboard deve indicar isso e não vincular o filtro de região a esses datasets. Os outros indicadores continuam usando região quando aplicável; tarifas usam a data de referência.

## Por que pode haver só três horas de clima

O seed versionado em `delta-mongo-database/scripts/delta-app/script-seed.js` contém um documento de 27/09/2026, `samples=3` e três horários UTC (00, 01 e 02). A Silver expande `hours[]`: não gera 24 horas e não transforma um seed em histórico mensal. Os três horários caem em 26/09 no fuso de São Paulo; `observed_at` governa a data analítica, não o nome do bucket.

Isso explica exatamente três linhas **se o ambiente estiver usando somente esse seed**. O código sozinho não comprova a contagem do Atlas/Databricks. `DIAGNOSTICO.sql` compara buckets, horas Silver e Gold, datas, temperaturas ausentes e pareamento com consumo. Não existe `LIMIT 3` nessas transformações.

Grafias equivalentes com medidas idênticas convergem sem duplicar horas. Medidas conflitantes na mesma cidade/hora bloqueiam a publicação; não se escolhe uma média arbitrária. Menos de dois dias gera aviso de insuficiência temporal nos metadados. Histórico adicional deve vir de coleta ou carga explicitamente identificada na origem, nunca de interpolação disfarçada na Gold.

## Publicação e evidência

Cada saída tem lista exata de colunas, chaves, valores obrigatórios e validações numéricas. Medidas negativas/não finitas, regiões conflitantes, cobertura acima do esperado, contadores de disponibilidade inconsistentes e campos extras impedem a gravação. `final_validity=NULL` significa tarifa sem fim definido e é permitido.

O notebook v2 prepara e valida as sete saídas antes de gravar qualquer tabela analítica. O pacote sem S3 faz preflight completo na mesma sessão. O orquestrador de notebooks separados faz uma passagem de validação antes da passagem de gravação; o vínculo técnico privado de UUIDs pode ser inicializado nesse preflight. Não se cria schema nem tabela adicional de qualidade.

Contagens, intervalo de datas, número de dias, estado EMPTY/VALID e avisos ficam nos logs e em `userMetadata.quality`, junto a `quality_contract_version=3`, lote e corte. EMPTY não significa zero medido. As sete escritas Delta não são uma transação: falha de armazenamento/permissão ainda pode deixar lote parcial. Somente consumir o conjunto após sucesso integral e comparação dos metadados. O código não publica ou corrige o dashboard automaticamente.

## Aplicação

1. Execute `DIAGNOSTICO.sql` no Databricks para medir o problema no lote real. Confira histórico/lote das Silver; as consultas de diagnóstico leem versões correntes.
2. Corrija cadastros/instrumentação que violarem o contrato na origem; recapture/reimporte o lote se necessário. Esta revisão não alterou Mongo, PostgreSQL ou dados remotos.
3. Substitua o notebook Gold pelo código revisado, execute o lote completo e confira logs/metadados das sete saídas.
4. Substitua os datasets pelos SELECTs de `CONSULTAS.sql` e use o prompt revisado. Os consumidores de `region_id`, `region_name` ou `user_key` em chamadas e região em metas precisam migrar junto.
5. Preserve o aviso de dados simulados até confirmar instrumentação real na origem. Não use nulidade zero ou novas medições fictícias para fazer um gráfico parecer completo.
