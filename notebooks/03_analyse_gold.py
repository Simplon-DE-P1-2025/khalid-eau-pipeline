# Databricks notebook source
# MAGIC %md
# MAGIC # Couche Gold - Analyse et KPIs de la Qualité de l'Eau
# MAGIC Agrégation des données nettoyées (Silver) pour créer des indicateurs métiers (Data Marts) prêts pour la Data Visualisation.

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, when, round

# 1. Définition des chemins
# Assure-toi que le chemin Silver correspond bien à celui utilisé dans le notebook 02
path_silver_plv = "/tmp/data/silver/hubeau_qualite_eau"
path_gold_kpi = "/tmp/data/gold/kpi_qualite_communes"

print("Lecture des données Silver...")
# On charge la table Silver (nettoyée)
df_silver = spark.read.format("delta").load(path_silver_plv)

# COMMAND ----------

# 2. Calcul des KPIs (Indicateurs de Performance)
print("Calcul des indicateurs de conformité par commune...")

df_gold_kpi = (
    df_silver
    # On groupe par code département et nom de la commune
    .groupBy("cddept", "nomcommuneprinc")
    .agg(
        # Nombre total de prélèvements effectués
        count("referenceprel").alias("total_prelevements"),
        # Nombre de prélèvements conformes en bactériologie (Valeur "C")
        sum(when(col("plvconformitebacterio") == "C", 1).otherwise(0)).alias(
            "conformes_bacterio"
        ),
        # Nombre de prélèvements conformes en chimie (Valeur "C")
        sum(when(col("plvconformitechimique") == "C", 1).otherwise(0)).alias(
            "conformes_chimique"
        ),
    )
    # 3. Calcul des pourcentages de conformité (arrondis à 2 décimales)
    .withColumn(
        "taux_conformite_bacterio_pct",
        round((col("conformes_bacterio") / col("total_prelevements")) * 100, 2),
    )
    .withColumn(
        "taux_conformite_chimique_pct",
        round((col("conformes_chimique") / col("total_prelevements")) * 100, 2),
    )
    # Tri : afficher les communes avec les taux de conformité les plus bas en premier (pour identifier les alertes)
    .orderBy("taux_conformite_bacterio_pct", "taux_conformite_chimique_pct")
)

# Aperçu du résultat
display(df_gold_kpi.limit(20))

# COMMAND ----------

# 4. Sauvegarde dans la couche Gold au format Delta
print(f"Sauvegarde de la table Gold dans {path_gold_kpi}...")

(
    df_gold_kpi.write.format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema", "true"
    )  # Permet de mettre à jour la structure de la table si on ajoute des KPIs plus tard
    .save(path_gold_kpi)
)

print("✅ Couche Gold générée avec succès ! Les données sont prêtes pour l'analyse.")
