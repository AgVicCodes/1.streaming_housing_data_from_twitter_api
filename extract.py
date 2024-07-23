import pandas as pd
import glob

csv_files = glob.glob(f"csv/*.{'csv'}")

df_append = pd.DataFrame()

for csv in csv_files:
    df_temp = pd.read_csv(csv)
    df_append = df_append._append(df_temp, ignore_index = True)

df_append.to_parquet("parquet/twitter_df.parquet")
df_append.to_csv("csv/twitter_df.csv")