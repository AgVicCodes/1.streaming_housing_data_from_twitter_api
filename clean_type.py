from transform import new_df
from transform import np
from transform import pd

new_df.loc[new_df["county"].isna(), "county"] = "United Kingdom"

rent_range = [0, 600, 1000, np.inf]

room_map = ["1", "2", "3"]

# print(new_df["rent_cost"].dtype)

new_df.loc[:, "rent_cost"] = new_df["rent_cost"].str.replace(",", "")
new_df.loc[:, "energy_bill"] = new_df["energy_bill"].str.replace(",", "")

# print(new_df.dtypes)

new_df.loc[:, "rent_cost"] = new_df["rent_cost"].astype("float")
new_df.loc[:, "energy_bill"] = new_df["energy_bill"].astype("float")
new_df.loc[:, "water"] = new_df["water"].astype("float")
new_df.loc[:, "council_tax"] = new_df["council_tax"].astype("float")
new_df.loc[:, "groceries"] = new_df["groceries"].astype("float")
new_df.loc[:, "clothing"] = new_df["clothing"].astype("float")

# print(new_df.dtypes)

subset_df = new_df.copy()

subset_df.loc[:, "room_no"] = pd.cut(new_df["rent_cost"], bins = rent_range, labels = room_map)

subset_df.loc[subset_df["no_of_rooms"].isna(), "no_of_rooms"] = subset_df["room_no"]

subset_df.loc[:, "no_of_rooms"] = subset_df["no_of_rooms"].astype("category")



# print(df[dup_first].head(20))

# na_rent = np.random.randint(int(new_df["rent_cost"].min()), int(new_df["rent_cost"].max()), new_df["rent_cost"].isna().sum())

# new_df.loc[new_df["rent_cost"].isna(), "rent_cost"] = na_rent


# print(new_df.head(10))
# print(new_df.sample(10))
# print((pseudo_energy_bills))

