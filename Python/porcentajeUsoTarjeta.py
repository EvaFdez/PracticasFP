# the pandas library is imported
import pandas as pd

# CSV file is read into a DataFrame
data = pd.read_csv(r'C:\Users\eva.fernandez.vegas\Downloads\PRACTICAS\unCSV.csv')

# the number of occurrences of each card is counted
account_cards = data['card'].value_counts()

# get the total number of transactions
total_transactions = len(data)

# results are displayed with the percent symbol
for card, account in account_cards.items():
    percentage = (account / total_transactions) * 100
    result = "{}: {:.2f}%".format(card, percentage)
    # {:.2f} specifies that the number be formatted with two decimal places, and % indicates that the percent symbol is added to the end
    print(result)
