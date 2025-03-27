# 道路安全数据仓库 - ETL处理程序 (使用列索引的健壮版本)
# Written by William Tai
# 导入必要的库
import pandas as pd
import numpy as np
import os
from datetime import datetime

# 记录开始时间
start_time = datetime.now()
print(f"ETL处理开始时间: {start_time}")

# 1. 提取(Extract)阶段 - 读取原始数据文件
print("\n========== 提取阶段 ==========")

# 读取致命事故数据
print("读取致命事故数据...")
fatal_crashes_df = pd.read_excel('bitre_fatal_crashes_dec2024.xlsx', sheet_name='BITRE_Fatal_Crash')
crash_count_df = pd.read_excel('bitre_fatal_crashes_dec2024.xlsx', sheet_name='BITRE_Fatal_Crash_Count_By_Date')

# 读取死亡人员数据
print("读取死亡人员数据...")
fatalities_df = pd.read_excel('bitre_fatalities_dec2024.xlsx', sheet_name='BITRE_Fatality')
fatality_count_df = pd.read_excel('bitre_fatalities_dec2024.xlsx', sheet_name='BITRE_Fatality_Count_By_Date')

# 读取住所数据 (跳过前11行，这些行包含元数据)
print("读取住所数据...")
try:
    lga_dwellings_df = pd.read_csv('LGA (count of dwellings).csv', skiprows=11)
    print(f"住所数据列数: {len(lga_dwellings_df.columns)}")
except Exception as e:
    print(f"读取住所数据时出错: {e}")
    try:
        # 尝试不同的方法
        lga_dwellings_df = pd.read_csv('LGA (count of dwellings).csv', header=None, skiprows=11)
        if len(lga_dwellings_df.columns) >= 2:
            lga_dwellings_df.columns = ['LGA_Name', 'Dwelling_Count'] + [f'Unnamed_{i}' for i in range(2, len(lga_dwellings_df.columns))]
        else:
            print("住所数据格式不正确")
            lga_dwellings_df = pd.DataFrame()
    except Exception as e2:
        print(f"尝试替代方法读取住所数据时出错: {e2}")
        lga_dwellings_df = pd.DataFrame()

# 记录原始数据行数
original_rows = {
    'fatal_crashes': len(fatal_crashes_df),
    'fatalities': len(fatalities_df),
    'crash_count': len(crash_count_df),
    'fatality_count': len(fatality_count_df),
    'lga_dwellings': len(lga_dwellings_df)
}

print("原始数据行数:")
for key, count in original_rows.items():
    print(f"{key}: {count}行")

# 2. 转换(Transform)阶段
print("\n========== 转换阶段 ==========")

# 打印列名信息以供参考
print("\n列名诊断:")
print(f"事故数据列数: {len(fatal_crashes_df.columns)}")
print(f"事故数据前5列: {fatal_crashes_df.columns[:5].tolist()}")
print(f"死亡人员数据前5列: {fatalities_df.columns[:5].tolist()}")
print(f"事故计数数据前5列: {crash_count_df.columns[:5].tolist()}")
print(f"死亡人员计数数据前5列: {fatality_count_df.columns[:5].tolist()}")

# 替换-9值为NaN(缺失值)
print("\n将-9值替换为NaN...")
fatal_crashes_df = fatal_crashes_df.replace(-9, np.nan)
fatalities_df = fatalities_df.replace(-9, np.nan)

# 检查替换后的缺失值情况
print("替换-9后的事故数据缺失值情况:")
null_counts = fatal_crashes_df.isnull().sum()
# 只打印前10个有缺失值的列
print(null_counts[null_counts > 0].head(10))

print("\n替换-9后的死亡人员数据缺失值情况:")
null_counts = fatalities_df.isnull().sum()
# 只打印前10个有缺失值的列
print(null_counts[null_counts > 0].head(10))

# 清洗事故数据
print("\n清洗事故数据...")
# 使用列索引而非列名
# 假设前5列是关键列: Crash ID, State, Month, Year, Dayweek
crash_key_indexes = [0, 1, 2, 3, 4]  # 列索引从0开始

# 检查关键列的缺失值
missing_key_rows = fatal_crashes_df.iloc[:, crash_key_indexes].isnull().any(axis=1)
missing_key_count = missing_key_rows.sum()
missing_key_pct = (missing_key_count / len(fatal_crashes_df)) * 100

print(f"事故数据中关键列有缺失值的行数: {missing_key_count}行 ({missing_key_pct:.2f}%)")

if missing_key_pct <= 5:
    # 安全删除关键列缺失的行
    cleaned_crashes_df = fatal_crashes_df[~missing_key_rows].copy()
    print(f"删除了关键列缺失的行，剩余{len(cleaned_crashes_df)}行")
else:
    # 不删除任何行，只填充缺失值
    cleaned_crashes_df = fatal_crashes_df.copy()
    # 对关键列填充缺失值
    for idx in crash_key_indexes:
        col = fatal_crashes_df.columns[idx]
        if idx in [2, 3]:  # Month, Year (数值型)
            cleaned_crashes_df.iloc[:, idx] = cleaned_crashes_df.iloc[:, idx].fillna(cleaned_crashes_df.iloc[:, idx].median())
        else:  # 分类型
            most_common = cleaned_crashes_df.iloc[:, idx].mode()[0] if not cleaned_crashes_df.iloc[:, idx].mode().empty else "Missing"
            cleaned_crashes_df.iloc[:, idx] = cleaned_crashes_df.iloc[:, idx].fillna(most_common)
    print(f"保留所有行，填充了缺失值")

# 清洗死亡人员数据
print("\n清洗死亡人员数据...")
# 假设前6列包含关键信息：Crash ID, State, Month, Year, Dayweek, Road User
fatality_key_indexes = [0, 1, 2, 3, 4, 11]  # 调整索引以匹配实际列位置

# 检查关键列的缺失值
missing_key_rows = fatalities_df.iloc[:, fatality_key_indexes].isnull().any(axis=1)
missing_key_count = missing_key_rows.sum()
missing_key_pct = (missing_key_count / len(fatalities_df)) * 100

print(f"死亡人员数据中关键列有缺失值的行数: {missing_key_count}行 ({missing_key_pct:.2f}%)")

if missing_key_pct <= 5:
    # 安全删除关键列缺失的行
    cleaned_fatalities_df = fatalities_df[~missing_key_rows].copy()
    print(f"删除了关键列缺失的行，剩余{len(cleaned_fatalities_df)}行")
else:
    # 不删除任何行，只填充缺失值
    cleaned_fatalities_df = fatalities_df.copy()
    # 对关键列填充缺失值
    for idx in fatality_key_indexes:
        col = fatalities_df.columns[idx]
        if idx in [2, 3]:  # Month, Year (数值型)
            cleaned_fatalities_df.iloc[:, idx] = cleaned_fatalities_df.iloc[:, idx].fillna(cleaned_fatalities_df.iloc[:, idx].median())
        else:  # 分类型
            most_common = cleaned_fatalities_df.iloc[:, idx].mode()[0] if not cleaned_fatalities_df.iloc[:, idx].mode().empty else "Missing"
            cleaned_fatalities_df.iloc[:, idx] = cleaned_fatalities_df.iloc[:, idx].fillna(most_common)
    print(f"保留所有行，填充了缺失值")

# 清洗住所数据
print("\n清洗住所数据...")
if not lga_dwellings_df.empty:
    # 假设第一列是LGA名称，第二列是住所数量
    if len(lga_dwellings_df.columns) >= 2:
        # 只保留前两列
        cleaned_lga_dwellings_df = lga_dwellings_df.iloc[:, :2].copy()
        # 重命名列
        cleaned_lga_dwellings_df.columns = ['LGA_Name', 'Dwelling_Count']
        # 删除缺失值行
        cleaned_lga_dwellings_df = cleaned_lga_dwellings_df.dropna()
        print(f"清洗后的住所数据: {len(cleaned_lga_dwellings_df)}行")
    else:
        print("住所数据列数不足")
        cleaned_lga_dwellings_df = lga_dwellings_df.copy()
else:
    print("住所数据为空")
    cleaned_lga_dwellings_df = pd.DataFrame()

# 3. 创建维度表和事实表
print("\n========== 创建维度表和事实表 ==========")

# 创建日期维度表
print("创建日期维度表...")
if len(crash_count_df.columns) >= 4:  # 确保有足够的列
    # 假设前4列是Date, Number of fatal crashes, Year, Month
    date_cols = [0, 2, 3]  # Date, Year, Month的索引
    
    # 从事故计数数据中提取日期信息
    dates_from_crash = crash_count_df.iloc[:, date_cols].copy()
    dates_from_crash.columns = ['Date', 'Year', 'Month']
    
    # 同样处理死亡人员计数数据
    if len(fatality_count_df.columns) >= 4:
        dates_from_fatality = fatality_count_df.iloc[:, date_cols].copy()
        dates_from_fatality.columns = ['Date', 'Year', 'Month']
        
        # 合并并去重
        all_dates = pd.concat([dates_from_crash, dates_from_fatality]).drop_duplicates().reset_index(drop=True)
    else:
        all_dates = dates_from_crash
    
    # 添加额外的时间维度信息
    all_dates['Month_Name'] = all_dates['Month'].apply(lambda x: {
        1: 'January', 2: 'February', 3: 'March', 4: 'April', 
        5: 'May', 6: 'June', 7: 'July', 8: 'August', 
        9: 'September', 10: 'October', 11: 'November', 12: 'December'
    }.get(x, 'Unknown'))
    
    all_dates['Quarter'] = all_dates['Month'].apply(lambda x: 
        'Q1' if x in [1, 2, 3] else 
        'Q2' if x in [4, 5, 6] else 
        'Q3' if x in [7, 8, 9] else 
        'Q4' if x in [10, 11, 12] else 'Unknown')

    # 检查数据结构
    print("all_dates列名:", all_dates.columns.tolist())
    print("all_dates前几行:", all_dates.head())

    # 确保Year和Month列是数值类型
    try:
        # 使用pd.to_numeric更安全地转换，errors='coerce'会将无效值转为NaN
        all_dates['Year'] = pd.to_numeric(all_dates['Year'], errors='coerce').fillna(0).astype(int)
        all_dates['Month'] = pd.to_numeric(all_dates['Month'], errors='coerce').fillna(0).astype(int)
        all_dates['Date_ID'] = all_dates['Year'] * 100 + all_dates['Month']
    except Exception as e:
        print(f"创建Date_ID时出错: {e}")
        # 创建一个默认值作为备选方案
        all_dates['Date_ID'] = 0
    
    # 如果存在Day Of Week列，添加它
    day_of_week_idx = -1
    for i, col in enumerate(crash_count_df.columns):
        if 'day' in str(col).lower() and 'week' in str(col).lower():
            day_of_week_idx = i
            break
    
    if day_of_week_idx != -1:
        # 创建日期到星期几的映射
        day_of_week_map = {}
        for idx, row in crash_count_df.iterrows():
            date = row.iloc[0]  # 第一列是日期
            day = row.iloc[day_of_week_idx]  # 星期几列
            day_of_week_map[date] = day
        
        all_dates['Day_Of_Week'] = all_dates['Date'].map(day_of_week_map)
    
    print(f"创建的日期维度表: {len(all_dates)}行")
    dim_date = all_dates
else:
    print("无法创建日期维度表，找不到必要的列")
    dim_date = pd.DataFrame()

# 创建地点维度表
print("创建地点维度表...")
# 收集地点相关列的索引
location_indexes = []
location_col_names = []

# 查找与地点相关的列索引
for i, col in enumerate(cleaned_crashes_df.columns):
    col_lower = str(col).lower()
    if any(term in col_lower for term in ['state', 'remote', 'area', 'sa4', 'lga', 'road type']):
        location_indexes.append(i)
        location_col_names.append(col)

if location_indexes:
    print(f"找到地点相关列: {location_col_names}")
    dim_location = cleaned_crashes_df.iloc[:, location_indexes].drop_duplicates().reset_index(drop=True)
    print(f"创建的地点维度表: {len(dim_location)}行")
else:
    print("无法找到地点相关列")
    dim_location = pd.DataFrame()

# 创建道路用户维度表
print("创建道路用户维度表...")
# 收集用户相关列的索引
user_indexes = []
user_col_names = []

# 查找与用户相关的列索引
for i, col in enumerate(cleaned_fatalities_df.columns):
    col_lower = str(col).lower()
    if any(term in col_lower for term in ['road user', 'gender', 'age']):
        user_indexes.append(i)
        user_col_names.append(col)

if user_indexes:
    print(f"找到道路用户相关列: {user_col_names}")
    dim_road_user = cleaned_fatalities_df.iloc[:, user_indexes].drop_duplicates().reset_index(drop=True)
    
    # 查找年龄列索引
    age_idx = -1
    for i, col in enumerate(dim_road_user.columns):
        if 'age' in str(col).lower() and 'group' not in str(col).lower():
            age_idx = i
            break
    
    # 如果有Age列但没有Age Group列，添加Age Group
    age_group_exists = any('age group' in str(col).lower() for col in dim_road_user.columns)
    if age_idx != -1 and not age_group_exists:
        dim_road_user['Age_Group'] = dim_road_user.iloc[:, age_idx].apply(lambda x: 
            '0_to_16' if pd.notnull(x) and x <= 16 else 
            '17_to_25' if pd.notnull(x) and 17 <= x <= 25 else 
            '26_to_39' if pd.notnull(x) and 26 <= x <= 39 else 
            '40_to_64' if pd.notnull(x) and 40 <= x <= 64 else 
            '65_to_74' if pd.notnull(x) and 65 <= x <= 74 else 
            '75_or_older' if pd.notnull(x) and x >= 75 else 'Unknown')
    
    print(f"创建的道路用户维度表: {len(dim_road_user)}行")
else:
    print("无法找到道路用户相关列")
    dim_road_user = pd.DataFrame()

# 创建事故类型维度表
print("创建事故类型维度表...")
# 收集事故类型相关列的索引
crash_type_indexes = []
crash_type_col_names = []

# 查找与事故类型相关的列索引
for i, col in enumerate(cleaned_crashes_df.columns):
    col_lower = str(col).lower()
    if any(term in col_lower for term in ['crash type', 'bus', 'truck', 'involvement', 'speed limit']):
        crash_type_indexes.append(i)
        crash_type_col_names.append(col)

if crash_type_indexes:
    print(f"找到事故类型相关列: {crash_type_col_names}")
    dim_crash_type = cleaned_crashes_df.iloc[:, crash_type_indexes].drop_duplicates().reset_index(drop=True)
    print(f"创建的事故类型维度表: {len(dim_crash_type)}行")
else:
    print("无法找到事故类型相关列")
    dim_crash_type = pd.DataFrame()

# 创建事实表 - 事故事实表
print("创建事故事实表...")
# 收集事实表相关列的索引
fact_indexes = []
fact_col_names = []

# 关键列索引集合
key_col_indexes = set([0, 1, 2, 3, 4])  # Crash ID, State, Month, Year, Dayweek

# 添加其他重要列
for i, col in enumerate(cleaned_crashes_df.columns):
    col_lower = str(col).lower()
    if i in key_col_indexes or any(term in col_lower for term in ['time', 'type', 'fatalities', 'speed', 'area']):
        fact_indexes.append(i)
        fact_col_names.append(col)

if fact_indexes:
    print(f"找到事故事实表相关列: {fact_col_names}")
    fact_crashes = cleaned_crashes_df.iloc[:, fact_indexes].copy()
    
    # 查找年份和月份列索引
    year_idx = -1
    month_idx = -1
    for i, col in enumerate(fact_crashes.columns):
        col_lower = str(col).lower()
        if 'year' in col_lower:
            year_idx = i
        elif 'month' in col_lower:
            month_idx = i
    
    # 创建日期ID
    if year_idx != -1 and month_idx != -1:
        fact_crashes['Date_ID'] = fact_crashes.iloc[:, year_idx].astype(int) * 100 + fact_crashes.iloc[:, month_idx].astype(int)
    
    print(f"创建的事故事实表: {len(fact_crashes)}行")
else:
    print("无法创建事故事实表，找不到必要的列")
    fact_crashes = pd.DataFrame()

# 创建死亡人员事实表
print("创建死亡人员事实表...")
# 收集事实表相关列的索引
fatality_fact_indexes = []
fatality_fact_col_names = []

# 关键列索引集合
key_col_indexes = set([0, 1, 2, 3, 4, 11])  # Crash ID, State, Month, Year, Dayweek, Road User

# 添加其他重要列
for i, col in enumerate(cleaned_fatalities_df.columns):
    col_lower = str(col).lower()
    if i in key_col_indexes or any(term in col_lower for term in ['time', 'gender', 'age', 'speed', 'area']):
        fatality_fact_indexes.append(i)
        fatality_fact_col_names.append(col)

if fatality_fact_indexes:
    print(f"找到死亡人员事实表相关列: {fatality_fact_col_names}")
    fact_fatalities = cleaned_fatalities_df.iloc[:, fatality_fact_indexes].copy()
    
    # 查找年份和月份列索引
    year_idx = -1
    month_idx = -1
    for i, col in enumerate(fact_fatalities.columns):
        col_lower = str(col).lower()
        if 'year' in col_lower:
            year_idx = i
        elif 'month' in col_lower:
            month_idx = i
    
    # 创建日期ID
    if year_idx != -1 and month_idx != -1:
        fact_fatalities['Date_ID'] = fact_fatalities.iloc[:, year_idx].astype(int) * 100 + fact_fatalities.iloc[:, month_idx].astype(int)
    
    print(f"创建的死亡人员事实表: {len(fact_fatalities)}行")
else:
    print("无法创建死亡人员事实表，找不到必要的列")
    fact_fatalities = pd.DataFrame()

# 4. 加载(Load)阶段 - 保存处理后的数据
print("\n========== 加载阶段 ==========")

# 创建输出目录
output_dir = 'processed_data'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 保存维度表
print("保存维度表...")
if not dim_date.empty:
    dim_date.to_csv(f'{output_dir}/dim_date.csv', index=False)
    print(f"保存了日期维度表: {len(dim_date)}行")

if not dim_location.empty:
    dim_location.to_csv(f'{output_dir}/dim_location.csv', index=False)
    print(f"保存了地点维度表: {len(dim_location)}行")

if not dim_road_user.empty:
    dim_road_user.to_csv(f'{output_dir}/dim_road_user.csv', index=False)
    print(f"保存了道路用户维度表: {len(dim_road_user)}行")

if not dim_crash_type.empty:
    dim_crash_type.to_csv(f'{output_dir}/dim_crash_type.csv', index=False)
    print(f"保存了事故类型维度表: {len(dim_crash_type)}行")

# 保存住所数据
if not cleaned_lga_dwellings_df.empty:
    cleaned_lga_dwellings_df.to_csv(f'{output_dir}/dim_dwellings.csv', index=False)
    print(f"保存了住所数据: {len(cleaned_lga_dwellings_df)}行")

# 保存事实表
print("保存事实表...")
if not fact_crashes.empty:
    fact_crashes.to_csv(f'{output_dir}/fact_crashes.csv', index=False)
    print(f"保存了事故事实表: {len(fact_crashes)}行")

if not fact_fatalities.empty:
    fact_fatalities.to_csv(f'{output_dir}/fact_fatalities.csv', index=False)
    print(f"保存了死亡人员事实表: {len(fact_fatalities)}行")

# 保存原始清洗数据
cleaned_crashes_df.to_csv(f'{output_dir}/cleaned_crashes.csv', index=False)
print(f"保存了清洗后的事故数据: {len(cleaned_crashes_df)}行")

cleaned_fatalities_df.to_csv(f'{output_dir}/cleaned_fatalities.csv', index=False)
print(f"保存了清洗后的死亡人员数据: {len(cleaned_fatalities_df)}行")

# 汇总统计
print("\n========== 数据处理汇总 ==========")
print("原始数据行数:")
for key, count in original_rows.items():
    print(f"{key}: {count}行")

print("\n清洗后数据行数:")
cleaned_rows = {
    'fatal_crashes': len(cleaned_crashes_df),
    'fatalities': len(cleaned_fatalities_df),
    'lga_dwellings': len(cleaned_lga_dwellings_df)
}

for key, count in cleaned_rows.items():
    if key in original_rows and original_rows[key] > 0:
        original = original_rows[key]
        removed = original - count
        removed_pct = (removed / original) * 100
        print(f"{key}: {count}行 (删除了{removed}行, {removed_pct:.2f}%)")

# 输出表统计
print("\n输出表统计:")
if 'dim_date' in locals() and not dim_date.empty:
    print(f"日期维度表: {len(dim_date)}行")
if 'dim_location' in locals() and not dim_location.empty:
    print(f"地点维度表: {len(dim_location)}行")
if 'dim_road_user' in locals() and not dim_road_user.empty:
    print(f"道路用户维度表: {len(dim_road_user)}行")
if 'dim_crash_type' in locals() and not dim_crash_type.empty:
    print(f"事故类型维度表: {len(dim_crash_type)}行")
if 'fact_crashes' in locals() and not fact_crashes.empty:
    print(f"事故事实表: {len(fact_crashes)}行")
if 'fact_fatalities' in locals() and not fact_fatalities.empty:
    print(f"死亡人员事实表: {len(fact_fatalities)}行")

# 记录结束时间
end_time = datetime.now()
duration = end_time - start_time
print(f"\nETL处理结束时间: {end_time}")
print(f"总处理时间: {duration}")

print("\nETL处理完成! 所有处理后的数据已保存到'{output_dir}'目录。")