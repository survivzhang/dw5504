#improt libararies
import pandas as pd

#Read the CSV file
header=['LGA','Dwelling Records','add']
transactions = pd.read_csv('LGA.csv', skiprows=12,header=None)
transactions.columns = header
transactions[transactions.columns[1]] = pd.to_numeric(transactions[transactions.columns[1]], errors='coerce')
transactions = transactions.dropna(subset=[transactions.columns[1]])
transactions.iloc[:, :2].to_csv('processed_LGA.csv', index=False)