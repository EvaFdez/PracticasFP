# the pandas library is imported
import pandas as pd

# CSV file is read into a DataFrame
data = pd.read_csv(r'C:\Users\eva.fernandez.vegas\Downloads\PRACTICAS\unCSV.csv')

# remove the euro symbol and replace the comma with a point in the 'amount' column
data['amount'] = data['amount'].str.replace('€', '').str.replace(',', '.')

# the values of the 'amount' column are converted to float type
data['amount'] = pd.to_numeric(data['amount'], errors='coerce')
# errors='coerce', means that if an error occurs during the conversion of some value to numeric,
# it will set that value to NaN (Not a Number) instead of raising an error

# the data are grouped by sex and the average expenditure is calculated
average_expense_sex = data.groupby('gender')['amount'].mean()
# mean() is a function used to calculate the mean (average) of the values

# the results are displayed on the screen
for sex, average_expense in average_expense_sex.items():
    average_spending_sex_rounded = round(average_expense, 2)
    average_expenditure_sex_euro = f"€{average_spending_sex_rounded}"
    # the letter "f" at the beginning of a text string indicates that an f-string format is being used in Python
    print(sex, average_expenditure_sex_euro)
