import pandas as pd
import sqlite3
conn = sqlite3.connect("gp_gmc_results.db")
df = pd.read_sql_query("SELECT distinct *  FROM gmc_data WHERE Registration_Status = 'Registered with a licence to practise'", conn)
print(df)