#Import necessary libraries
import pandas as pd
import numpy as np
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth  # 替换 apriori
from mlxtend.frequent_patterns import association_rules
import string

# 读取数据
df2 = pd.read_excel("bitre_fatalities_dec2024.xlsx",sheet_name="BITRE_Fatality")
df2.to_csv("temp1.csv", index=False)
df2 = pd.read_csv("temp1.csv",skiprows=2,header=2, dtype=str)  # 使用dtype=str确保所有列都是字符串类型
new_df = df2.drop(['Crash ID', 'Age', 'SA4 Name 2021', 'National LGA Name 2021' ],axis = 'columns')
cleaned_df = new_df

# 用英文字母做前缀
prefixes = list(string.ascii_uppercase)

# 遍历每一列，给所有元素加上对应前缀
for idx, col in enumerate(cleaned_df.columns):
    prefix = prefixes[idx % len(prefixes)]
    cleaned_df[col] = prefix + '_' + cleaned_df[col].astype(str)

# 转换为交易格式
my_list = cleaned_df.values.tolist()

# 转换为布尔数组
te = TransactionEncoder()
array_te = te.fit(my_list).transform(my_list)

# 转换为DataFrame
arm_df = pd.DataFrame(array_te, columns = te.columns_)

# 使用FP-Growth算法找出频繁项集
frequent_itemsets = fpgrowth(arm_df, 
                           min_support=0.2,  # 最小支持度
                           use_colnames=True)

# 计算规则长度
frequent_itemsets['length'] = frequent_itemsets['itemsets'].apply(lambda x: len(x))

# 筛选长度为2且支持度>=0.2的项集
length_2_itemsets = frequent_itemsets[(frequent_itemsets['length']==2) & 
                                    (frequent_itemsets['support']>=0.2)]

# 生成关联规则
rules = association_rules(frequent_itemsets, 
                        metric="confidence",
                        min_threshold=0.5)  # 最小置信度

# 选择需要的指标
result_arm = rules[['antecedents','consequents','support','confidence','lift']]

# 筛选置信度>=0.7的规则
new_result_arm = result_arm[result_arm['confidence']>=0.7]

# 按照confidence降序排序
new_result_arm = new_result_arm.sort_values('confidence', ascending=False)

# 保存结果
new_result_arm.to_csv("fpgrowth_rules.csv", index=False)

# 打印结果和统计信息
print("FP-Growth关联规则分析结果：")
print(new_result_arm)
print("\n基本统计信息：")
print("规则总数:", len(new_result_arm))
print("平均置信度:", round(new_result_arm['confidence'].mean(), 3))
print("平均支持度:", round(new_result_arm['support'].mean(), 3))
print("平均提升度:", round(new_result_arm['lift'].mean(), 3))

# 可以添加其他评估指标
new_result_arm['conviction'] = np.where(new_result_arm['confidence']==1, 
                                      float('inf'), 
                                      (1-new_result_arm['support'])/
                                      (1-new_result_arm['confidence']))

# 打印前10个规则的详细信息
print("\n前10个最强关联规则：")
print(new_result_arm.head(10))