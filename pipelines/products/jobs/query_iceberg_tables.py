from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_TABLE = "lakehouse.silver.products"
GOLD_TABLE = "lakehouse.gold.product_category_summary"


def build_spark():
    return (
        SparkSession.builder
        .appName("ucv-query-iceberg")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config(
            "spark.sql.catalog.lakehouse.warehouse",
            "s3a://ucv-data-platform/products"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://floci:4566")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .getOrCreate()
    )


spark = build_spark()

# Leer las tablas Iceberg
silver = spark.table(SILVER_TABLE)
gold = spark.table(GOLD_TABLE)

print("\n=== TABLA SILVER ===")
silver.printSchema()
silver.show(20, truncate=False)

silver_rows = silver.count()
print("Silver rows:", silver_rows)

print("\n=== TABLA GOLD ===")
gold.printSchema()
gold.show(50, truncate=False)

gold_rows = gold.count()
print("Gold rows:", gold_rows)

# Validaciones de calidad en Silver
silver_null_ids = silver.filter(
    F.col("product_id").isNull()
).count()

silver_invalid_prices = silver.filter(
    F.col("price").isNull() | (F.col("price") <= 0)
).count()

silver_duplicates = (
    silver.groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Validaciones de calidad en Gold
gold_null_categories = gold.filter(
    F.col("category").isNull()
).count()

gold_invalid_counts = gold.filter(
    F.col("product_count") <= 0
).count()

gold_invalid_prices = gold.filter(
    F.col("average_price").isNull() |
    (F.col("average_price") <= 0)
).count()

gold_duplicate_categories = (
    gold.groupBy("category")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

gold_product_total = gold.agg(
    F.sum("product_count").alias("total")
).first()["total"] or 0

print("\n=== VALIDACIONES DE CALIDAD ===")
print("Silver product_id nulos:", silver_null_ids)
print("Silver precios inválidos:", silver_invalid_prices)
print("Silver productos duplicados:", silver_duplicates)
print("Gold categorías nulas:", gold_null_categories)
print("Gold product_count inválidos:", gold_invalid_counts)
print("Gold precios promedio inválidos:", gold_invalid_prices)
print("Gold categorías duplicadas:", gold_duplicate_categories)
print("Total productos contabilizados en Gold:", gold_product_total)

checks = [
    silver_null_ids == 0,
    silver_invalid_prices == 0,
    silver_duplicates == 0,
    gold_null_categories == 0,
    gold_invalid_counts == 0,
    gold_invalid_prices == 0,
    gold_duplicate_categories == 0,
    gold_product_total == silver_rows
]

if all(checks):
    print("\nRESULTADO: TODAS LAS VALIDACIONES APROBADAS")
else:
    print("\nRESULTADO: HAY VALIDACIONES PENDIENTES")

spark.stop()