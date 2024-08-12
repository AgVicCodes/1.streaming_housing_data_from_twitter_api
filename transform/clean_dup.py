from clean_type import subset_df
from clean_type import np
from clean_type import pd

# rent_outliers = subset_df["rent_cost"] > 3000

# energy_outliers = subset_df["energy_bill"] > 1000

# df = subset_df[~rent_outliers]

df = subset_df.copy()

subset = ["rent_cost", "energy_bill"]

# dup_first = df.duplicated(subset = ["username", "rent_cost", "no_of_rooms"], keep = "first")

df = df.drop_duplicates(subset = "username", keep = "first")

df = df.dropna(subset = subset, how = "all")

# print(df.loc[df["energy_bill"] > 500])
# print(df.sample(10))