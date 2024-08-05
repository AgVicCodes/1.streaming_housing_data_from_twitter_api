from transform import new_df
from transform import np
from transform import pd

new_df.loc[new_df["county"].isna(), "county"] = "United Kingdom"

rent_range = [0, 600, 1000, np.inf]

room_map = ["1", "2", "3"]

print(new_df["rent_cost"].dtype)

new_df["rent_cost"] = new_df["rent_cost"].str.replace(",", "")
new_df["energy_bill"] = new_df["energy_bill"].str.replace(",", "")

print(new_df.dtypes)

new_df["no_of_rooms"] = new_df["no_of_rooms"].astype("float") if new_df["no_of_rooms"].any() != '' else np.NaN
new_df["rent_cost"] = new_df["rent_cost"].astype("float")
new_df["energy_bill"] = new_df["energy_bill"].astype("float")
new_df["water"] = new_df["water"].astype("float")
new_df["council_tax"] = new_df["council_tax"].astype("float")
new_df["groceries"] = new_df["groceries"].astype("float")
new_df["clothing"] = new_df["clothing"].astype("float")

print(new_df.dtypes)

# new_df["rent_cost"] = new_df["rent_cost"].astype("float")

new_df["room_no"] = pd.cut(new_df["rent_cost"], bins = rent_range, labels = room_map)

new_df.loc[new_df["no_of_rooms"].isna(), "no_of_rooms"] = new_df["room_no"]


# new_df["energy_bill"].str.replace(",", "").astype("float").fillna((new_df["energy_bill"].mean()), inplace = True)



print(new_df.head(10))

