import pandas as pd
from extract.extract import merge_csv
from extract.extract_regex import extract_regex
from transform.clean_type import string_to_float
from transform.replace_nan import populate_df
from transform.clean_dup import remove_na_dup
from transform.check_na import convert_type
from load.load_data import load_data

merge_csv()

df = extract_regex()

df = string_to_float(df)

df = populate_df(df)

df = remove_na_dup(df)

df = convert_type(df)

load_data(df)

clean_df = pd.read_parquet("data/parquet/clean_df.parquet", engine = "pyarrow")

print(clean_df.head())