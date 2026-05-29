# Databricks notebook source
#dbutils.fs.mkdirs("/Workspace/dados")
#dbutils.fs.ls("/Workspace")


# COMMAND ----------

# Criando catálogo e schemas

spark.sql("CREATE CATALOG IF NOT EXISTS inbound")

spark.sql("CREATE SCHEMA IF NOT EXISTS inbound.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS inbound.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS inbound.gold")

# COMMAND ----------

# MAGIC %md
# MAGIC # DEPOIS QUE FIZ UPLOAD RODEI ISTO AQUI

# COMMAND ----------

df = spark.read.json(
    "/Volumes/inbound/bronze/raw/dados_brutos_imoveis.json"
)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salvar o bronze
# MAGIC

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("inbound.bronze.imoveis_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Depois você criará Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS inbound.silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criar inbound gold
# MAGIC

# COMMAND ----------

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS inbound.gold"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Depois disso Você poderá salvar tabelas assim: 
# MAGIC Bronze

# COMMAND ----------

df.write.saveAsTable(
    "inbound.bronze.imoveis_raw"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Já existia, overwrite

# COMMAND ----------

df.write \
  .mode("overwrite") \
  .saveAsTable("inbound.bronze.imoveis_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize

# COMMAND ----------

display(
    spark.table("inbound.bronze.imoveis_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

# MAGIC %md
# MAGIC limpa

# COMMAND ----------

df_silver = df.dropna()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC cria silveer

# COMMAND ----------

from pyspark.sql.functions import col, get

df_silver = df.select(
    col("anuncio.id").alias("id_imovel"),
    col("anuncio.tipo_anuncio").alias("tipo_anuncio"),
    col("anuncio.tipo_unidade").alias("tipo_unidade"),
    col("anuncio.tipo_uso").alias("tipo_uso"),

    col("anuncio.endereco.cidade").alias("cidade"),
    col("anuncio.endereco.estado").alias("estado"),
    col("anuncio.endereco.bairro").alias("bairro"),
    col("anuncio.endereco.cep").alias("cep"),

    col("usuario.nome").alias("usuario"),

    get(col("anuncio.quartos"), 0).alias("quartos"),
    get(col("anuncio.banheiros"), 0).alias("banheiros"),
    get(col("anuncio.vaga"), 0).alias("vagas"),

    get(col("anuncio.area_total"), 0).alias("area_total"),

    get(col("anuncio.valores"), 0)["valor"].alias("valor")
)

# COMMAND ----------

# MAGIC %md
# MAGIC visualize

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC apaga caso exista

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS inbound.silver.imoveis_tratado
""")

# COMMAND ----------

# MAGIC %md
# MAGIC salva

# COMMAND ----------

df_silver.write \
    .mode("overwrite") \
    .saveAsTable("inbound.silver.imoveis_tratado")

# COMMAND ----------

# MAGIC %md
# MAGIC visualiza

# COMMAND ----------

display(
    spark.table("inbound.silver.imoveis_tratado")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold

# COMMAND ----------

from pyspark.sql.functions import avg, regexp_replace

df_gold = (
    df_silver
    .withColumn(
        "valor_num",
        regexp_replace("valor", "[^0-9]", "").cast("double")
    )
    .groupBy("cidade")
    .agg(
        avg("valor_num").alias("preco_medio")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC salvar gold

# COMMAND ----------

df_gold.write \
    .mode("overwrite") \
    .saveAsTable("inbound.gold.preco_medio_cidade")

# COMMAND ----------

# MAGIC %md
# MAGIC visualizar gold

# COMMAND ----------

display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC visualizar schema (isto poderia ser feito lá no começo)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Listar dbfs

# COMMAND ----------

display(
    dbutils.fs.ls("/Volumes/inbound/bronze/raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC caminho completo

# COMMAND ----------

for f in dbutils.fs.ls("/Volumes/inbound/bronze/raw"):
    print(f.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ler json

# COMMAND ----------

df = spark.read.json(
    "/Volumes/inbound/bronze/raw/dados_brutos_imoveis.json"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ler csv

# COMMAND ----------

df = spark.read.csv(
    "/Volumes/inbound/bronze/raw/arquivo.csv",
    header=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks moderno
# MAGIC
# MAGIC /Volumes/
# MAGIC
# MAGIC Unity Catalog
# MAGIC
# MAGIC Managed Volumes

# COMMAND ----------


