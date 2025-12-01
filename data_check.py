import pandas as pd
import sqlite3
conn = sqlite3.connect("gmc_results.db")
df = pd.read_sql_query("SELECT * FROM gmc_data", conn)
print(df)