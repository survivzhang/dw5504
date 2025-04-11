#Import necessary libraries, pandas, numpy and mlxtend
import pandas as pd
import numpy as np
import mlxtend
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

df2 = pd.read_excel("bitre_fatalities_dec2024.xlsx",sheet_name="BITRE_Fatality")
df2.to_csv("temp1.csv", index=False)
df2 = pd.read_csv("temp1.csv",skiprows=2,header=2)
new_df = df2.drop(['Crash ID', 'Age', 'SA4 Name 2021', 'National LGA Name 2021' ],axis = 'columns')
cleaned_df = new_df
#Data Transformation

cleaned_df = cleaned_df.astype(str)

import string

# 用英文字母做前缀，比如 A, B, C, ...
prefixes = list(string.ascii_uppercase)

# 遍历每一列，给所有元素加上对应前缀
for idx, col in enumerate(cleaned_df.columns):
    prefix = prefixes[idx % len(prefixes)]  # 防止列数超过26，循环使用字母
    cleaned_df[col] = prefix + '_' + cleaned_df[col].astype(str)


#TransactionEncoder() was designed to covert lists to array
my_list = cleaned_df.values.tolist()

#Covert the my_list to one-hot encoded boolean numpy array. 
#Apriori function allows boolean data type only, such as 1 and 0, or FALSE and TRUE.
te = TransactionEncoder()
array_te = te.fit(my_list).transform(my_list)

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

#Assume the length is 2 and the min support is >= 0.3
frequent_itemsets[ (frequent_itemsets['length']==2) & 
                  (frequent_itemsets['support']>=0.2)]

#Assume the min confidence is 0.5
rules_con = association_rules(frequent_itemsets, metric="confidence",min_threshold=0.5)

#Assume the min lift is 1
rules_lift = association_rules(frequent_itemsets, metric="lift",min_threshold=1)

#Based on min confidence (=0.5), 
#output antecedents, consequents, support, confidence and lift.
result_arm = rules_con[['antecedents','consequents','support','confidence','lift']]

#Find the rules whose confidence >= 0.7
new_result_arm = result_arm[result_arm['confidence']>=0.7]

# Save the result to CSV
new_result_arm.to_csv("association_rules.csv", index=False)
new_result_arm