from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_DIR = Path(__file__).resolve().parent.parent
FLUME_DIR = PROJECT_DIR / "output" / "flume"
OUTPUT_DIR = PROJECT_DIR / "output" / "analytics"

COLUMNS = [
    "Country",
    "Year",
    "Attack Type",
    "Target Industry",
    "Financial Loss (in Million $)",
    "Number of Affected Users",
    "Attack Source",
    "Security Vulnerability Type",
    "Defense Mechanism Used",
    "Incident Resolution Time (in Hours)",
]


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("GlobalCybersecurityThreatAnalytics")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Flume's File Roll Sink creates files without CSV headers.
        raw = spark.read.text(str(FLUME_DIR)).select(
            F.split(F.col("value"), ",").alias("fields")
        )

        df = raw.select(
            *[
                F.col("fields").getItem(i).alias(name)
                for i, name in enumerate(COLUMNS)
            ]
        )

        df = (
            df.withColumn("Year", F.col("Year").cast("int"))
            .withColumn(
                "Financial Loss (in Million $)",
                F.col("Financial Loss (in Million $)").cast("double"),
            )
            .withColumn(
                "Number of Affected Users",
                F.col("Number of Affected Users").cast("long"),
            )
            .withColumn(
                "Incident Resolution Time (in Hours)",
                F.col("Incident Resolution Time (in Hours)").cast("double"),
            )
        )

        # Remove empty sink-roll files and malformed rows.
        df = df.filter(
            F.col("Country").isNotNull()
            & (F.col("Country") != "")
            & F.col("Year").isNotNull()
            & F.col("Financial Loss (in Million $)").isNotNull()
            & F.col("Number of Affected Users").isNotNull()
            & F.col("Incident Resolution Time (in Hours)").isNotNull()
        ).cache()

        total = df.count()
        if total == 0:
            raise RuntimeError("No valid records found in Flume output.")

        print(f"\nValid records processed by PySpark: {total}")
        print("\n=== Overall Summary ===")
        df.agg(
            F.count("*").alias("Total Incidents"),
            F.round(
                F.sum("Financial Loss (in Million $)") / 1000, 2
            ).alias("Financial Loss (Billion USD)"),
            F.sum("Number of Affected Users").alias("Affected Users"),
            F.round(
                F.avg("Incident Resolution Time (in Hours)"), 2
            ).alias("Average Resolution Time (Hours)"),
            F.countDistinct("Country").alias("Countries Affected"),
        ).show(truncate=False)

        aggregations = {
            "attacks_by_year": ["Year"],
            "attacks_by_country": ["Country"],
            "attacks_by_type": ["Attack Type"],
            "attacks_by_industry": ["Target Industry"],
            "attacks_by_source": ["Attack Source"],
            "attacks_by_vulnerability": ["Security Vulnerability Type"],
            "attacks_by_defense": ["Defense Mechanism Used"],
        }

        for output_name, group_columns in aggregations.items():
            result = df.groupBy(*group_columns).agg(
                F.count("*").alias("Incident Count"),
                F.round(
                    F.sum("Financial Loss (in Million $)"), 2
                ).alias("Financial Loss (Million USD)"),
                F.sum("Number of Affected Users").alias("Affected Users"),
                F.round(
                    F.avg("Incident Resolution Time (in Hours)"), 2
                ).alias("Avg Resolution Time (Hours)"),
            ).orderBy(*group_columns)

            destination = OUTPUT_DIR / output_name
            result.coalesce(1).write.mode("overwrite").option(
                "header", True
            ).csv(str(destination))

            print(f"Saved: {destination}")

        print("\n=== Attacks by Year ===")
        df.groupBy("Year").count().orderBy("Year").show(20, truncate=False)
        print("\nPySpark analytics completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
