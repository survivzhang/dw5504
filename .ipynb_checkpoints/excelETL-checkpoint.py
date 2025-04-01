import pandas as pd
df = pd.read_excel("bitre_fatal_crashes_dec2024.xlsx",sheet_name="BITRE_Fatal_Crash")
df.to_csv("temp.csv", index=False)
df = pd.read_csv("temp.csv")
print(df.head(10))