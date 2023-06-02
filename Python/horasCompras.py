# the pandas library is imported
import pandas as pd

# CSV file is read into a DataFrame
data = pd.read_csv(r'C:\Users\eva.fernandez.vegas\Downloads\PRACTICAS\unCSV.csv')

# the 'timestamp' column is converted to the time format
data['timestamp'] = pd.to_datetime(data['timestamp'], format='%H:%M:%S')
# pd.to_datetime() is a pandas function that converts column values to datetime objects
# format='%H:%M:%S' specifies the format of the time values in the column

# data is grouped by country and time and counts the number of shopping in each group
purchases_per_hour_country = data.groupby(['country', data['timestamp'].dt.hour])['timestamp'].count()
# dt.hour is an attribute of the pandas DatetimeIndex class that is used to extract the hour portion of a datetime object
# ['timestamp'].count() is applied after grouping and counts the number of occurrences of the 'timestamp' column in each group

# the time with the maximum and minimum amount of purchases is sought for each country
hour_max_purchases = purchases_per_hour_country.groupby('country').idxmax()
hour_min_purchases = purchases_per_hour_country.groupby('country').idxmin()

# results are printed
for country, max_hour in hour_max_purchases.items():
    print(f"The time of day with the most purchases in {country} is: {max_hour[1]}:00")
# {max_hour[1]} is replaced by the value at position 1 of the max_hour variable
# :00 is appended to the end to indicate that the value of max_hour[1] is displayed followed by ":00",
# which represents an hour in HH:00 format

for country, min_hour in hour_min_purchases.items():
    print(f"The time of day with the least purchases in {country} is: {min_hour[1]}:00")