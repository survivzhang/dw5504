import pandas as pd
import numpy as np
import mlxtend
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

# Read the CSV file
df = pd.read_csv('total.csv')

# Drop the column 'Crash ID'
new_df = df.drop('Crash ID', axis='columns')

# Data transformation: TransactionEncoder() function only handles string type
new_df = new_df.astype(str)

# Convert the DataFrame to a list of lists (transactions)
transaction_list = new_df.values.tolist()

# Convert the list to one-hot encoded boolean numpy array
te = TransactionEncoder()
array_te = te.fit(transaction_list).transform(transaction_list)

# Convert the array to a DataFrame
arm_df = pd.DataFrame(array_te, columns=te.columns_)

# Find the frequent itemsets
frequent_itemsets = apriori(arm_df, min_support=0.2, use_colnames=True)

# Check the length of rules
frequent_itemsets['length'] = frequent_itemsets['itemsets'].apply(lambda x: len(x))

# Filter itemsets based on length and support
filtered_itemsets = frequent_itemsets[(frequent_itemsets['length'] == 2) & (frequent_itemsets['support'] >= 0.2)]

# Define function to map itemsets to column names and ensure uniqueness
def map_itemset_to_column_names(itemset):
    # Convert frozenset to list and map it directly to column names
    return sorted(list(itemset))  # Sort to ensure consistency in column ordering and avoid duplicates

# Map the itemsets to their corresponding column names and add a new column
filtered_itemsets['itemsets_column_names'] = filtered_itemsets['itemsets'].apply(lambda x: map_itemset_to_column_names(x))

# Save the filtered frequent itemsets to a CSV file
filtered_itemsets.to_csv('filtered_frequent_itemsets.csv', index=False)

# Show the resulting DataFrame with itemsets mapped to column names
print(filtered_itemsets)
