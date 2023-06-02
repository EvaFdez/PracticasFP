# the pandas library is imported
import pandas as pd

# CSV file is read into a DataFrame
data = pd.read_csv(r'C:\Users\eva.fernandez.vegas\Downloads\PRACTICAS\unCSV.csv')

# the number of transactions is counted.
num_total_transactions = data['client'].count()

# It could also be done with shape that returns the number of elements in the array
# num_total_transactions = data.shape[0]

# the number is formatted to make it look better when printed
num_total_transactions_formatted = format(num_total_transactions, ",")

# is displayed on the screen
print("Total number of transactions: ", num_total_transactions_formatted)