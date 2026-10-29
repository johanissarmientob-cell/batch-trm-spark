"""
Procesamiento en batch con Apache Spark (PySpark)
Dataset: Tasa de Cambio Representativa del Mercado (TRM) - Historico
Fuente: datos.gov.co (Superintendencia Financiera de Colombia)

Etapas: carga desde HDFS, limpieza, analisis exploratorio (EDA) y visualizacion.
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("BatchTRM").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. CARGA desde HDFS
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
    .load("hdfs://localhost:9000/Tarea3/rows.csv")
print("=== ESQUEMA ===")
df.printSchema()
print("Registros originales:", df.count())

# 2. LIMPIEZA
df = df.dropna(subset=["VALOR", "VIGENCIADESDE"]).dropDuplicates()
df = df.withColumn("FECHA", F.to_date("VIGENCIADESDE", "dd/MM/yyyy")) \
       .withColumn("ANIO", F.year("FECHA"))
df = df.filter(F.col("FECHA").isNotNull())
print("Registros despues de limpiar:", df.count())

# 3. ANALISIS EXPLORATORIO (EDA)
print("=== ESTADISTICAS ===")
df.select("VALOR").summary().show()

print("=== TRM por anio (promedio, minimo, maximo) ===")
por_anio = df.groupBy("ANIO").agg(
    F.round(F.avg("VALOR"), 2).alias("PROMEDIO"),
    F.min("VALOR").alias("MINIMO"),
    F.max("VALOR").alias("MAXIMO")).orderBy("ANIO")
por_anio.show(50)

print("=== Top 10 valores mas altos ===")
df.orderBy(F.col("VALOR").desc()).select("VALOR", "VIGENCIADESDE", "VIGENCIAHASTA").show(10)

print("=== Dias con TRM mayor a 5000 ===")
df.filter(F.col("VALOR") > 5000).select("VALOR", "VIGENCIADESDE").show()

# 4. VISUALIZACION
pdf = por_anio.toPandas()
plt.figure(figsize=(11, 5))
plt.plot(pdf["ANIO"], pdf["PROMEDIO"], marker="o", label="Promedio")
plt.fill_between(pdf["ANIO"], pdf["MINIMO"], pdf["MAXIMO"], alpha=0.2, label="Rango min-max")
plt.title("TRM promedio anual (COP por USD)")
plt.xlabel("Anio"); plt.ylabel("COP"); plt.grid(True); plt.legend()
plt.savefig("trm_grafica.png", dpi=120)
print("Grafica guardada en trm_grafica.png")
