# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
print("test")

# COMMAND ----------

print("testa se include in exports funcionou")

# COMMAND ----------

# MAGIC %md
# MAGIC pip install databricks-cli
# MAGIC
# MAGIC databricks configure --token   # informe host + token pessoal
# MAGIC
# MAGIC databricks bundle deploy --target dev
# MAGIC

# COMMAND ----------

# testa dbfs
dbutils.fs.ls("/")


# COMMAND ----------

#Criar um arquivo de teste
# 
dbutils.fs.put(
    "dbfs:/Workspace/teste.txt",
    "Olá DBFS!",
    True
)

# COMMAND ----------

#Ler o arquivo
dbutils.fs.head("dbfs:/Workspace/teste.txt")

# COMMAND ----------


