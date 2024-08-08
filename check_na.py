from replace_nan import df



df["no_of_rooms"] = df["no_of_rooms"].astype("category")
df["rent_cost"] = df["rent_cost"].astype("float")
df["energy_bill"] = df["energy_bill"].astype("float")
df["water"] = df["water"].astype("float")
df["council_tax"] = df["council_tax"].astype("float")
df["groceries"] = df["groceries"].astype("float")
df["clothing"] = df["clothing"].astype("float")

# print(df[df["no_of_rooms"] == ""])


print(df.info())
print(df.describe(include = "all"))
# print(df["no_of_rooms"].unique())
print(df[df["username"] == "SK 🇬🇧🇬🇧"])