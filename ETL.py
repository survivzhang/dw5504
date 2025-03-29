#improt libararies
import pandas as pd

#Read the CSV file
header=['LGA','Dwelling Records','add']
transactions = pd.read_csv('LGA.csv', skiprows=12,header=None)
transactions.columns = header
transactions.to_csv('processed_LGA.csv', index=False)