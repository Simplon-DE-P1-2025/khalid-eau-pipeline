# Databricks notebook source
# MAGIC %md
# MAGIC # Couche Silver - Nettoyage des données Qualité de l'eau
# MAGIC Transformation des données brutes (Bronze) en données typées et propres (Silver).

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, trim, upper
from delta.tables import DeltaTable

# 1. Définition des chemins
path_bronze_plv = "/tmp/data/bronze/hubeau_qualite_eau"
path_silver_plv = "/tmp/data/silver/prelevements"

print("Lecture des données Bronze...")
df_bronze = spark.read.format("delta").load(path_bronze_plv)

# COMMAND ----------

# 2. Nettoyage et typage
print("Application des transformations Silver...")

df_silver = (
    df_bronze
    # 2.1 La vraie clé primaire est la combinaison du prélèvement ET du paramètre mesuré
    .dropDuplicates(["code_prelevement", "code_parametre"])
    # 2.2 Nettoyage des espaces et standardisation en majuscules
    .withColumn("code_departement", trim(col("code_departement")))
    .withColumn("nom_commune", trim(upper(col("nom_commune"))))
    .withColumn(
        "conclusion_conformite_prelevement",
        trim(col("conclusion_conformite_prelevement")),
    )
    # 2.3 Typage de la date (L'API renvoie du format ISO8601, Spark le gère nativement)
    .withColumn("timestamp_prelevement", to_timestamp(col("date_prelevement")))
    # 2.4 Filtrage de sécurité : on exclut les lignes sans identifiant de prélèvement
    .filter(col("code_prelevement").isNotNull() & (col("code_prelevement") != ""))
)

# Aperçu du résultat
display(df_silver.limit(10))

# COMMAND ----------

# 3. Sauvegarde en Delta Lake (Silver)
print(f"Sauvegarde dans {path_silver_plv}...")

# Optimisation : Remplacement du Overwrite (destructeur) par un MERGE (Upsert)
if DeltaTable.isDeltaTable(spark, path_silver_plv):
    print("Mise à jour incrémentale (MERGE) de la table Silver...")
    delta_target = DeltaTable.forPath(spark, path_silver_plv)
    (
        delta_target.alias("target")
        .merge(
            df_silver.alias("source"),
            "target.code_prelevement = source.code_prelevement AND target.code_parametre = source.code_parametre",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    print("Première création de la table Silver...")
    df_silver.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(path_silver_plv)

print("✅ Traitement Silver terminé avec succès !")
