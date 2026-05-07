import sys
import os
from pathlib import Path


def main():
    print("Pipeline Data Engineering - Qualité de l'Eau")
    print("=" * 50)

    # Ajouter le répertoire racine au chemin Python
    root_dir = Path(__file__).parent
    sys.path.insert(0, str(root_dir))

    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("eau-pipeline")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )

        print("\nSparkSession initialized successfully")
        print("\nNote: This is a local testing script.")
        print("For production, use Databricks notebooks directly.")
        print("\nAvailable notebooks:")
        print("  1. notebooks/01_ingestion_bronze.py     - Ingestion API")
        print("  2. notebooks/02_nettoyage_silver.py     - Nettoyage et transformation")
        print("  3. notebooks/03_analyse_gold.py         - Analyse et KPIs")

    except ImportError:
        print("\nError: PySpark not installed locally.")
        print("Run: pip install -r requirements.txt")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
