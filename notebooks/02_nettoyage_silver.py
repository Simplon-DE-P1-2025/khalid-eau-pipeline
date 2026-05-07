# Databricks notebook source
# MAGIC %md
# MAGIC # Couche Silver - Nettoyage des données Qualité de l'eau
# MAGIC Transformation des données brutes (Bronze) en données typées et propres (Silver).

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    to_date,
    to_timestamp,
    concat_ws,
    regexp_replace,
    trim,
    upper,
)

# 1. Définition des chemins (à adapter selon ton architecture)
# Supposons que tu as sauvegardé tes données Bronze au format Delta
path_bronze_plv = "/tmp/data/bronze/hubeau_qualite_eau"
path_silver_plv = "/tmp/data/silver/hubeau_qualite_eau"

# Si tu lis directement les CSV/TXT pour tester, décommente la ligne ci-dessous et commente la lecture Delta :
# df_bronze = spark.read.csv("/FileStore/tables/DIS_PLV_*.txt", header=True, sep=",", quote='"')

print("Lecture des données Bronze...")
df_bronze = spark.read.format("delta").load(path_bronze_plv)

# COMMAND ----------

# 2. Nettoyage et typage
print("Application des transformations Silver...")

df_silver = (
    df_bronze
    # 2.1 Suppression des doublons stricts
    .dropDuplicates(["referenceprel"])
    # 2.2 Nettoyage des espaces sur les colonnes textuelles clés
    .withColumn("cddept", trim(col("cddept")))
    .withColumn("nomcommuneprinc", trim(upper(col("nomcommuneprinc"))))
    .withColumn("conclusionprel", trim(col("conclusionprel")))
    # 2.3 Typage de la date (format attendu : yyyy-MM-dd)
    .withColumn("date_prelevement", to_date(col("dateprel"), "yyyy-MM-dd"))
    # 2.4 Transformation de l'heure (remplacement du 'h' par ':' pour avoir HH:mm)
    .withColumn("heureprel_clean", regexp_replace(col("heureprel"), "h", ":"))
    # 2.5 Création d'un vrai Timestamp combinant la date et l'heure
    .withColumn(
        "timestamp_prelevement",
        to_timestamp(
            concat_ws(" ", col("date_prelevement"), col("heureprel_clean")),
            "yyyy-MM-dd HH:mm",
        ),
    )
    # 2.6 Nettoyage de la colonne 'pourcentdebit' (retirer le symbole '%' et caster en float si possible)
    .withColumn(
        "pourcentdebit_clean",
        regexp_replace(col("pourcentdebit"), " %", "").cast("float"),
    )
    # 2.7 Nettoyage : retirer les colonnes brutes inutiles ou redondantes
    .drop("dateprel", "heureprel", "heureprel_clean", "pourcentdebit")
    # 2.8 Filtrage des lignes corrompues (ex: pas de référence de prélèvement)
    .filter(col("referenceprel").isNotNull() & (col("referenceprel") != ""))
)

# Aperçu du résultat
display(df_silver.limit(10))

# COMMAND ----------

# 3. Sauvegarde en Delta Lake (Silver)
print(f"Sauvegarde dans {path_silver_plv}...")

(
    df_silver.write.format("delta")
    .mode("overwrite")  # On écrase et remplace pour garantir l'idempotence
    .option("overwriteSchema", "true")
    .save(path_silver_plv)
)

print("✅ Traitement Silver terminé avec succès !")
