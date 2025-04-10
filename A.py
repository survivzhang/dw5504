#Import necessary libraries, pandas, numpy and mlxtend
import pandas as pd
import numpy as np
import mlxtend
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

#Read the csv file
df = pd.read_csv('total.csv')

#print the df
print(df)

#Show the data head
df.head()

#Check the data types in the dataframe
df.dtypes

#The columns of the dataframe
df.columns

#Check the number of rows and columns
df.shape
#Drop the column, 'id'.
new_df = df.drop('Crash ID',axis = 'columns')
#Data transformation
#TransactionEncoder() function only can handle string type
new_df = new_df.astype(str)

#TransactionEncoder() was designed to covert lists to array
list = new_df.values.tolist()

#Covert the list to one-hot encoded boolean numpy array. 
#Apriori function allows boolean data type only, such as 1 and 0, or FALSE and TRUE.
te = TransactionEncoder()
array_te = te.fit(list).transform(list)

#Check the array
array_te

#Check the colunms
te.columns_

#Apriori function can handle dataframe only, covert the array to a dataframe
arm_df = pd.DataFrame(array_te, columns = te.columns_)

#Find the frequent itemsets
frequent_itemsets = apriori(arm_df,min_support=0.2,use_colnames =True)

#Check the length of rules
frequent_itemsets['length']=frequent_itemsets['itemsets'].apply(lambda x: len(x))

# 假设 frequent_itemsets 是你的频繁项集 DataFrame
filtered_itemsets = frequent_itemsets[(frequent_itemsets['length'] == 2) & (frequent_itemsets['support'] >= 0.2)]

# 将筛选后的频繁项集输出为 CSV 文件
filtered_itemsets.to_csv('filtered_frequent_itemsets.csv', index=False)
# 生成关联规则，使用提升度（lift）作为评估标准
