# Import SparkSession
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

# Input data sets are read and placed in a data frame
input_df = spark.read.option("header", True).csv("resources/exercise.csv")

# a) Numero de transacciones totales del dia
num_transactions = input_df.count()

# The number is formatted to have the thousands separator
num_transactions_formatted = "{:,}".format(num_transactions)
print("Numero de transacciones totales del dia:", num_transactions_formatted)

# b) Calculo de la media de gasto por sexo
# The original values are replaced by numeric values thanks to the regular expression,
# in addition those values are passed to decimal numbers without the euro symbol
df_formatted = input_df.withColumn("amount", regexp_extract(col("amount"), r"(\d+(\.\d+)?)", 1).cast(DoubleType()))

# The data are grouped by sex and then the average is calculated with two decimal places
average_expenditure_sex = df_formatted.groupby("gender").agg(round(avg("amount"), 2).alias("average_expense"))

# Results are displayed
average_expenditure_sex.show()

# c) Calculo de la media de gasto por pais (España, Argentina, Francia, Alemania, Brasil, Portugal) y total (independiente del pais)
df_formatted = input_df.withColumn("amount", regexp_extract(col("amount"), r"(\d+(\.\d+)?)", 1).cast(DoubleType()))
country_average_expenditure = df_formatted.groupby("country").agg(round(avg("amount"), 2).alias("average_expense"))
average_expenditure_total_countries = df_formatted.agg(
    round(avg("amount"), 2).alias("average_expenditure_total_countries"))
country_average_expenditure.show()
average_expenditure_total_countries.show()

# d) Los 5 servicios donde se ha producido mas gasto en: España, Argentina y Alemania
# The countries are filtered
df_filtered = df_formatted.filter(df_formatted.country.isin("Spain", "Argentina", "Germany"))

# The total cost per service in each country is calculated
total_expenditure_by_service = df_filtered.groupby("country", "retail_service").agg(
    sum("amount").alias("total_expenditure"))

# Rank is added to each service by country based on total spend, top five services are filtered out, then rank column is removed
largest_services_by_country = total_expenditure_by_service.withColumn("rank", dense_rank().over(
    Window.partitionBy("country").orderBy(desc("total_expenditure")))
                                                                      ).filter(col("rank") <= 5).drop("rank")
# Results are displayed
largest_services_by_country.show()

# e) Cual es el porcentaje de uso de cada tipo de tarjeta (Visa, Mastercard, AmericanExpress)
# The DataFrame is filtered by Visa, Mastercard and AmericanExpress cards
df_filtered = input_df.filter(input_df.card.isin("visa", "mastercard", "americanexpress"))

# The total number of transactions for each type of card is calculated
card_counts = df_filtered.groupby("card").agg(count("*").alias("total_transactions"))

# The percentage of use of each type of card is calculated
total_transactions = df_filtered.count()
card_percentages = card_counts.withColumn("percentage",
                                          round(card_counts.total_transactions / total_transactions * 100, 2))

# The percent symbol is added to the end of the value
card_percentages = card_percentages.withColumn("percentage", concat(card_percentages.percentage, lit("%")))

# Results are displayed
card_percentages.show()

# f) Cual es la hora del dia a la que mas se compra? Y a la que menos? - Para cada uno de los 6 paises
# The time of day is extracted
df_hour = input_df.withColumn("hour_of_day", concat(hour("timestamp"), lit(":00")))

# The purchase count is calculated by country and time of day
shopping_time = df_hour.groupBy("country", "hour_of_day").count()

# A row number is assigned to each hour of the day within each country based on the purchase count
window_spec = Window.partitionBy("country").orderBy(desc("count"))
ranked_hours = shopping_time.withColumn("rank", row_number().over(window_spec))

# The hours of the day with the highest and lowest number of purchases are filtered for each country
max_hours = ranked_hours.filter(col("rank") == 1).drop("rank").withColumnRenamed("hour_of_day", "most_purchased_hour")
min_hours = ranked_hours.filter(col("rank") == 24).drop("rank").withColumnRenamed("hour_of_day", "least_purchased_hour")

# The desired columns are selected to display the results
max_hours = max_hours.select("country", "most_purchased_hour")
min_hours = min_hours.select("country", "least_purchased_hour")

# Results are displayed
max_hours.show()
min_hours.show()
