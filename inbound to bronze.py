# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
for f in dbutils.fs.ls("/Volumes/inbound/bronze/raw"):
    print(f.path)

# COMMAND ----------

val path ="/Volumes/inbound/bronze/raw"
val dados = spark.read.json(path)

# COMMAND ----------

schemas = spark.sql("SHOW SCHEMAS IN inbound").collect()

for schema in schemas:
    schema_name = schema["databaseName"]

    print(f"\n📁 SCHEMA: {schema_name}")

    try:
        volumes = spark.sql(
            f"SHOW VOLUMES IN inbound.{schema_name}"
        ).collect()

        if len(volumes) == 0:
            print("   └── (sem volumes)")

        for volume in volumes:
            print(f"   📦 {volume['volume_name']}")

    except Exception as e:
        print(f"   ❌ Erro: {e}")

# COMMAND ----------

dados = "dbfs:/Volumes/inbound/bronze/raw/dados_brutos_imoveis.json"

# COMMAND ----------

df = spark.read.json(dados)

display(df)

# COMMAND ----------

df = df.drop(imagens, usuario)

# COMMAND ----------

# remove coluna de imagens e ids
df = df.drop(columns=["imagens", "usuario"], errors="ignore")

# COMMAND ----------

# MAGIC %md
# MAGIC # BRONZE AULA 4 SEÇÃO 4 

# COMMAND ----------

# MAGIC %md
# MAGIC verificar arquivos inbound

# COMMAND ----------

display(
    dbutils.fs.ls("/Volumes/inbound/bronze/raw")
)

# COMMAND ----------

### Ler JSON

# COMMAND ----------

df = spark.read.json(
    "/Volumes/inbound/bronze/raw/dados_brutos_imoveis.json"
)

# COMMAND ----------

# MAGIC %md
# MAGIC visualizar

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC schema

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Remover as colunas "imagens" e "usuario"

# COMMAND ----------

df_limpo = df.drop("imagens", "usuario")

# COMMAND ----------

# MAGIC %md
# MAGIC Criar a coluna ID

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC Extraia o ID que está dentro de anuncio:

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.json(
    "/Volumes/inbound/bronze/raw/dados_brutos_imoveis.json"
)

df_bronze = (
    df
    .drop("imagens", "usuario")
    .withColumn("id", col("anuncio.id"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC conferir

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC verificar

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Salvar na camada Bronze em formato Delta

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC grave em delta
# MAGIC

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .save("/Volumes/inbound/bronze/raw/imoveis")
)

# COMMAND ----------

# MAGIC %md
# MAGIC CONFERE

# COMMAND ----------

#display(df_bronze)
display(dbutils.fs.ls("/Volumes/inbound/bronze/raw/imoveis"))


# COMMAND ----------

# MAGIC %md
# MAGIC Transformação de endereço

# COMMAND ----------

df_endereco = df_bronze.select("anuncio.endereco")

path = "/Volumes/inbound/bronze/raw/dataset_endereco"

(
    df_endereco.write
    .format("parquet")
    .mode("overwrite")
    .save(path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # SILVER AULA 7 SECAO 4

# COMMAND ----------

# =====================================
# 1. Ler os dados da camada Bronze
# =====================================

path_bronze = "/Volumes/inbound/bronze/raw/imoveis"

df_bronze = (
    spark.read
    .format("delta")
    .load(path_bronze)
)

display(df_bronze)

# COMMAND ----------

# =====================================
# 2. Expandir o JSON da coluna anuncio
# =====================================

df_anuncio = df_bronze.select("anuncio.*")

display(df_anuncio)

# COMMAND ----------

# =====================================
# 3. Expandir os campos do endereço
# =====================================

df_silver = (
    df_bronze
    .select(
        "anuncio.*",
        "anuncio.endereco.*"
    )
)

display(df_silver)

# COMMAND ----------

# =====================================
# 4. Remover colunas desnecessárias
# =====================================

df_silver = (
    df_silver
    .drop("caracteristicas")
    .drop("endereco")
)

display(df_silver)

# COMMAND ----------

# =====================================
# 5. Verificar schema final
# =====================================

df_silver.printSchema()

# COMMAND ----------

# =====================================
# 6. Salvar na camada Silver
# =====================================

path_silver = "/Volumes/inbound/bronze/raw/silver/imoveis"

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .save(path_silver)
)

# COMMAND ----------

# =====================================
# 7. Validar a gravação
# =====================================

df_validacao = (
    spark.read
    .format("delta")
    .load(path_silver)
)

display(df_validacao)

# COMMAND ----------

display(
    dbutils.fs.ls(
        "/Volumes/inbound/bronze/raw/silver/imoveis"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Obter os nomes das colunas

# COMMAND ----------

column_names = df_silver.columns

column_names

# COMMAND ----------

# MAGIC %md
# MAGIC Verificar o tipo da variável

# COMMAND ----------

column_names = df_silver.columns

type(column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC Imprimir cada coluna individualmente

# COMMAND ----------

for col_name in column_names:
    print(col_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Exibir colunas numeradas

# COMMAND ----------

for i, col_name in enumerate(df_silver.columns, start=1):
    print(f"{i}. {col_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Converter para DataFrame para visualização no Databricks

# COMMAND ----------

colunas_df = spark.createDataFrame(
    [(c,) for c in df_silver.columns],
    ["nome_coluna"]
)

display(colunas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Ver quantidade de colunas

# COMMAND ----------

len(df_silver.columns)

# COMMAND ----------


