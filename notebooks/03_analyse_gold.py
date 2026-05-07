# Databricks notebook source
# MAGIC %md
# MAGIC # Couche Gold - Analyse et KPIs de la Qualité de l'Eau
# MAGIC Agrégation des données nettoyées (Silver) pour créer des indicateurs métiers.

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, when, round
from delta.tables import DeltaTable

# 1. Définition des chemins
path_silver_plv = "/tmp/data/silver/prelevements"
path_gold_kpi = "/tmp/data/gold/kpi_qualite_communes"

print("Lecture des données Silver...")
df_silver = spark.read.format("delta").load(path_silver_plv)

# COMMAND ----------

# 2. Préparation des données uniques
# Une analyse d'eau (1 code_prelevement) contient plusieurs paramètres.
# Pour calculer la conformité globale, on isole 1 ligne par prélèvement.
df_unique_samples = df_silver.select(
    "code_departement",
    "nom_commune",
    "code_prelevement",
    "conformite_limites_bact_prelevement",
    "conformite_limites_pc_prelevement",
).dropDuplicates(["code_prelevement"])


# 3. Calcul des KPIs (Indicateurs de Performance)
print("Calcul des indicateurs de conformité par commune...")

df_gold_kpi = (
    df_unique_samples.groupBy("code_departement", "nom_commune")
    .agg(
        # Nombre total de prélèvements uniques
        count("code_prelevement").alias("total_prelevements"),
        # Comptage des prélèvements conformes en bactériologie (Valeur "C")
        sum(
            when(col("conformite_limites_bact_prelevement") == "C", 1).otherwise(0)
        ).alias("conformes_bacterio"),
        # Comptage des prélèvements conformes en physico-chimie (Valeur "C")
        sum(
            when(col("conformite_limites_pc_prelevement") == "C", 1).otherwise(0)
        ).alias("conformes_chimique"),
    )
    # Calcul des pourcentages
    .withColumn(
        "taux_conformite_bacterio_pct",
        round((col("conformes_bacterio") / col("total_prelevements")) * 100, 2),
    )
    .withColumn(
        "taux_conformite_chimique_pct",
        round((col("conformes_chimique") / col("total_prelevements")) * 100, 2),
    )
    # Tri : Les pires taux en premier pour alerter
    .orderBy("taux_conformite_bacterio_pct", "taux_conformite_chimique_pct")
)

# Aperçu du résultat
display(df_gold_kpi.limit(20))

# COMMAND ----------

# 4. Sauvegarde dans la couche Gold au format Delta
print(f"Sauvegarde de la table Gold dans {path_gold_kpi}...")

# Optimisation : Remplacement du Overwrite par un MERGE (Upsert) pour la table KPI
if DeltaTable.isDeltaTable(spark, path_gold_kpi):
    print("Mise à jour incrémentale (MERGE) de la table Gold...")
    delta_target = DeltaTable.forPath(spark, path_gold_kpi)
    (
        delta_target.alias("target")
        .merge(
            df_gold_kpi.alias("source"),
            "target.code_departement = source.code_departement AND target.nom_commune = source.nom_commune",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    print("Première création de la table Gold...")
    df_gold_kpi.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(path_gold_kpi)

print("✅ Traitement Gold terminé avec succès !")
