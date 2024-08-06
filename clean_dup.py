from clean_type import subset_df
from clean_type import np
from clean_type import pd

rent_outliers = subset_df["rent_cost"] > 4000

df = subset_df[~rent_outliers]

subset = ["rent_cost", "energy_bill"]

# dup_first = df.duplicated(subset = ["username", "rent_cost", "no_of_rooms"], keep = "first")

df = df.drop_duplicates(subset = "username", keep = "first")

# df = df.dropna(subset = subset, how = "all")

print(df.count())