# the pandas library is imported
import pandas as pd

# CSV file is read into a DataFrame
data = pd.read_csv(r'C:\Users\eva.fernandez.vegas\Downloads\PRACTICAS\unCSV.csv')

data['amount'] = data['amount'].str.replace('€', '').str.replace(',', '.')
data['amount'] = pd.to_numeric(data['amount'], errors='coerce')
average_expense_country = data.groupby('country')['amount'].mean()

for country, average_expense in average_expense_country.items():
    average_expense_country_rounded = round(average_expense, 2)
    average_expense_country_euro = f"€{average_expense_country_rounded}"
    print(country, average_expense_country_euro)

# the total average cost is calculated
average_expense_total = data['amount'].mean()

# the total average cost is rounded to two decimal places
average_expense_total_rounded = round(average_expense_total, 2)

# the total average expenditure is formatted with the euro symbol
average_expense_total_euro = f"€{average_expense_total_rounded}"

# the total average spend is shown
print("Average total spend:", average_expense_total_euro)
