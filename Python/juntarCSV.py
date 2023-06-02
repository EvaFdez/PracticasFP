# the pandas library is imported
import pandas as pd
# the pandas library is very useful for working with tabular data, such as CSV files

import glob
# glob is a module in Python that provides a way to perform file name pattern searches in a directory

# directory where the CSV files are located
directory = r'C:\Users\eva.fernandez.vegas\Documents\variosCSV'

# all CSV files in the given directory are read
csv_files = glob.glob(directory + '/*.csv')

# list to store the data of each CSV file
data = []

# reads each CSV file and adds its data to the list
for archive in csv_files:
    data.append(pd.read_csv(archive))

# data from all files is combined into a single DataFrame
combined_data = pd.concat(data, ignore_index=True)

# save the merged DataFrame to a new CSV file
combined_data.to_csv(r'C:\Users\eva.fernandez.vegas\Documents\unCSV.csv', index=False)

print("CSV files successfully combined")
