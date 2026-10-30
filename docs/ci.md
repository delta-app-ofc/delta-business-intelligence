# Validação técnica

Em pull requests e por acionamento manual, o GitHub Actions verifica a sintaxe dos arquivos Python de `v1` com Python 3.9, sem executá-los. Para repetir localmente com Python 3.9:

```sh
python -m compileall -q v1
```

Essa verificação não testa comportamento, consultas SQL, dependências ou execução dos jobs no Glue e no Databricks, nem acessa bancos ou serviços externos.
