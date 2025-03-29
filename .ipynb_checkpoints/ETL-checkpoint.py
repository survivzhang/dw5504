#improt libararies
import pandas as pd

#Read the CSV file
transactions = pd.read_csv('transactions.csv')
transactions.head(3)