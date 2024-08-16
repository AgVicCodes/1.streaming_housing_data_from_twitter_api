import pandas as pd
import datetime
import time
from extract import merge_csv
from extract import extract_data
from extract import extract_tweets
from transform import string_to_float
from transform import populate_df
from transform import remove_na_dup
from transform import convert_type
from load import load_data


print(f"Starting data pipeline at {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}")
print("_____________________________________________")

# Step 1: Authenticate the twitter API and perform query search
t0 = time.time()
extract_tweets()
t1 = time.time()
print(f"Completed Step 1")
print(f"---> Process completed in {str(t1 - t0)} seconds \n")


# Step 2: Merge the data files from step 1
t0 = time.time()
merge_csv()
t1 = time.time()
print(f"Completed Step 2")
print(f"---> Storage files merged in {str(t1 - t0)} seconds \n")


# Step 3: Convert the file to a DataFrame 
t0 = time.time()
df = extract_data()
t1 = time.time()
print(f"Completed Step 3")
print(f"---> Extracted data with regex in {str(t1 - t0)} seconds \n")


# Step 4: Convert data types
t0 = time.time()
df = string_to_float(df)
t1 = time.time()
print(f"Completed Step 4")
print(f"---> Converted to appropriate data types in {str(t1 - t0)} seconds \n")


# Step 5: Replace null values
t0 = time.time()
df = populate_df(df)
t1 = time.time()
print(f"Completed Step 5")
print(f"---> Null values replaced in {str(t1 - t0)} seconds \n")


# Step 6: Remove null values and duplicates
t0 = time.time()
df = remove_na_dup(df)
t1 = time.time()
print(f"Completed Step 6")
print(f"---> Removed null and duplicate values in {str(t1 - t0)} seconds \n")


# Step 7: Reconvert data types for persistence 
t0 = time.time()
df = convert_type(df)
t1 = time.time()
print(f"Completed Step 7")
print(f"---> Converted data types in {str(t1 - t0)} seconds \n")


# Step 8: Save the DataFrame to file
t0 = time.time()
load_data(df)
t1 = time.time()
print(f"Completed Step 8")
print(f"---> Saved DataFrame to file in {str(t1 - t0)} seconds \n")


# Step 9: Preview DataFrame
t0 = time.time()
clean_df = pd.read_parquet("data/parquet/clean_df.parquet", engine = "pyarrow")
print(clean_df.head())
t1 = time.time()
print(f"Completed Step 9")
print(f"---> Loaded and previewed DataFrame in {str(t1 - t0)} seconds \n")