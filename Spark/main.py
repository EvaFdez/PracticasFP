# Import SparkSession
from pyspark.sql import SparkSession, Window
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

# Base path of the "resources" directory
base_path = os.path.join(os.getcwd(), "resources")
# os is the Python module for operating system path and directory manipulation
# os.getcwd() is a function that returns the path of the Current Working Directory (CWD)
# os.path.join() is a function used to join parts of paths into a single path

# Get the list of folders inside "resources"
folders = os.listdir(base_path)

# Process each folder
total_transactions = 0
total_amount = 0.0

for folder in folders:
    folder_path = os.path.join(base_path, folder)
    csv_files = [file for file in os.listdir(folder_path) if file.endswith(".csv")]

    for csv_file in csv_files:
        file_path = os.path.join(folder_path, csv_file)
        input_df = spark.read.csv(file_path, header=True, inferSchema=True)
        num_transactions = input_df.count()
        amount_sum = \
            input_df.select(sum(regexp_extract(col("amount"), r"(\d+(\.\d+)?)", 1).cast(DoubleType()))).first()[0]
        total_transactions += num_transactions
        total_amount += amount_sum

# a) Numero de transacciones totales de la semana
total_transactions_formatted = "{:,}".format(total_transactions)
print("Transacciones totales de la semana:", total_transactions_formatted)

# b) Calculo de la media de gasto por sexo
average_expense_sex = spark.read.csv(file_path, header=True, inferSchema=True).groupBy("gender").agg(
    round(avg(regexp_extract(col("amount"), r"(\d+(\.\d+)?)", 1).cast(DoubleType())), 2).alias("average_expense"))
average_expense_sex = average_expense_sex.withColumn("average_expense", concat(col("average_expense"), lit("€")))
print("Media de gasto por sexo de la semana:")
average_expense_sex.show()

# c) Calculo de la media de gasto por pais (España, Argentina, Francia, Alemania, Brasil, Portugal) y total (independiente del pais)
average_expense_country = spark.read.csv(file_path, header=True, inferSchema=True).groupBy("country").agg(
    round(avg(regexp_extract(col("amount"), r"(\d+(\.\d+)?)", 1).cast(DoubleType())), 2).alias("average_expense"))
average_expense_country = average_expense_country.withColumn("average_expense",
                                                             concat(col("average_expense"), lit("€")))
print("Media de gasto por pais de la semana:")
average_expense_country.show()
average_expenditure_total_countries = spark.read.csv(file_path, header=True, inferSchema=True).agg(
    round(avg(regexp_extract(col("amount"), r"(\d+(\.\d+)?)", 1).cast(DoubleType())), 2).alias("average_expense_total"))
average_expenditure_total_countries = average_expenditure_total_countries.withColumn("average_expense_total",
                                                                                     concat(
                                                                                         col("average_expense_total"),
                                                                                         lit("€")))
print("Gasto medio total de todos los paises de la semana:")
average_expenditure_total_countries.show()

# d) Los 5 servicios donde se ha producido mas gasto en: España, Argentina y Alemania
country_filter = ["Spain", "Argentina", "Germany"]

top_services = spark.read.csv(file_path, header=True, inferSchema=True) \
    .filter(col("country").isin(country_filter)) \
    .groupBy("country", "retail_service") \
    .agg(sum(regexp_extract(col("amount"), r"(\d+(\.\d+)?)", 1).cast(DoubleType())).alias("total_expenditure")) \
    .orderBy("country", desc("total_expenditure"))
top_services_euro = top_services.withColumn("total_expenditure", concat(col("total_expenditure"), lit("€")))

for country in country_filter:
    print(f"Servicios donde se ha producido mas gasto en {country}:")
    # f at the beginning of the string indicates that it is a Python-formatted string, also known as f-string
    top_services_euro.filter(col("country") == country).limit(5).show()

# e) Cual es el porcentaje de uso de cada tipo de tarjeta (visa, mastercard, americanexpress)

# f) Cual es la hora de la semana a la que mas se compra? Y a la que menos? - Para cada uno de los 6 paises
