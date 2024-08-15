import pandas as pd
from extract import merge_csv
from extract import extract_data
from transform import string_to_float
from transform import populate_df
from transform import remove_na_dup
from transform import convert_type
from load import load_data

merge_csv()

df = extract_data()

df = string_to_float(df)

df = populate_df(df)

df = remove_na_dup(df)

df = convert_type(df)

load_data(df)

clean_df = pd.read_parquet("data/parquet/clean_df.parquet", engine = "pyarrow")

print(clean_df.head())