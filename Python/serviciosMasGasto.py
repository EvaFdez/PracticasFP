# the pandas library is imported
import pandas as pd

# CSV file is read into a DataFrame
data = pd.read_csv(r'C:\Users\eva.fernandez.vegas\Downloads\PRACTICAS\unCSV.csv')

# the data is filtered by the desired countries
countries_searched = ['Spain', 'Argentina', 'Germany']
leaked_data = data[data['country'].isin(countries_searched)]
# isin() is a pandas string method used to check if string elements are present in a list or in another string
# is a boolean indexing operation that filters the DataFrame 'data' and returns a new DataFrame containing only the rows
# where the value of the column 'country' is present in the list 'countries_searched'

# convert column "amount" to numeric type
leaked_data['amount'] = leaked_data['amount'].str.replace('€', '').str.replace(',', '.')
leaked_data['amount'] = pd.to_numeric(leaked_data['amount'], errors='coerce')

# the total cost by service and country is calculated
total_expense_service_country = leaked_data.groupby(['retail_service', 'country'])['amount'].sum()

# the 5 services with the highest expenditure in each country are obtained
for country in countries_searched:
    services_country = total_expense_service_country.loc[:, country].nlargest(5)
    # loc is a pandas indexing method used to access and select data from a DataFrame or Series.
    # It is used to filter rows and columns based on specific tags or conditions.
    # nlargest(5) is a pandas function that is applied to an array and returns the 5 largest values in descending order
    print("Country:", country)
    for service, amount in services_country.items():
        formatted_amount = "{:,.2f}".format(amount)
        print(service, formatted_amount)
    print()
