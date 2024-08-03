import numpy as np
import pandas as pd
import re


rent_regex = r"(?:[Rr]ent|[Bb]edroom|[Bb]d|[Bb]ed|[Rr]oom\s)\s?[:-]?\s?£?(\d?,?\d{3,4})"

energy_regex = r"[Ee]nergy\s?[:-]*(?:\D+\s?)?\s?£?(\d?,?\d+)+"

water_regex = r"[Ww]ater\s?[:-]*\s?£?(\d?,?\d+)+"

tax_regex = r"[Cc]ouncil\s?[Tt]ax\s?[:-]*\s?£?(\d?,?\d+)+"

groceries_regex = r"[Gg]roceries\s?[:-]*\s?£?(\d?,?\d+)+"

clothing_regex = r"[Cc]lothing\s?[:-]*\s?£?(\d?,?\d+)+"

room_regex = r"(\d)?\s?(?:[Bb]edroom|[Bb]d|[Bb]ed|[Rr]oom)"

county_regex = r"(?:Aberdeen|Bedford|Birmingham|Bolton|Bristol|Canterbury|Cambridgeshire|Coventry|Dartford|Derby|Dundee|Durham|Essex|Glasgow|Gloucester|Gosport|Ireland|Leeds|Leicester|Lincoln|London|Loughborough|Luton|Manchester|Middlesbrough|Northampton|Oxford|Peterborough|Scotland|Sheffield|Stoke|Sunderland|Surrey|Swanley|Walsall|Westminster|Wolverhampton)\b"

df = pd.read_parquet("parquet/twitter_df.parquet", engine = "fastparquet")

df["text"] = df["text"].str.replace("\n", " ")

df["county"] = df["text"].apply(lambda x: re.findall(county_regex, x)[0] if re.findall(county_regex, x) else np.NaN)

df["no_of_rooms"] = df["text"].apply(lambda x: re.findall(room_regex, x)[0] if re.findall(room_regex, x) else np.NaN)

df["rent_cost"] = df["text"].apply(lambda x: re.findall(rent_regex, x)[0] if re.findall(rent_regex, x) else np.NaN)

df["energy_bill"] = df["text"].apply(lambda x: re.findall(energy_regex, x)[0] if re.findall(energy_regex, x) else np.NaN)

df["water"] = df["text"].apply(lambda x: re.findall(water_regex, x)[0] if re.findall(water_regex, x) else np.NaN)

df["council_tax"] = df["text"].apply(lambda x: re.findall(tax_regex, x)[0] if re.findall(tax_regex, x) else np.NaN)

df["groceries"] = df["text"].apply(lambda x: re.findall(groceries_regex, x)[0] if re.findall(groceries_regex, x) else np.NaN)

df["clothing"] = df["text"].apply(lambda x: re.findall(clothing_regex, x)[0] if re.findall(clothing_regex, x) else np.NaN)  

new_df = df[["username", "county", "no_of_rooms", "rent_cost", "energy_bill", "water", "council_tax", "groceries", "clothing"]]

new_df.loc[new_df["county"].isna(), "county"] = "United Kingdom"

rent_range = [0, 600, 1000, np.inf]

room_map = [1, 2, 3]

print(new_df["rent_cost"].dtype)

new_df["rent_cost"] = new_df["rent_cost"].str.replace(",", "")

new_df["rent_cost"] = new_df["rent_cost"].astype("float")

print(new_df["rent_cost"].dtype)

# new_df["rent_cost"] = new_df["rent_cost"].astype("float")

new_df["room_no"] = pd.cut(new_df["rent_cost"], bins = rent_range, labels = room_map)

# new_df.loc[new_df["no_of_rooms"].isna(), "no_of_rooms"] = new_df["room_no"]

new_df["no_of_rooms"].fillna(pd.cut(new_df["rent_cost"], bins = rent_range, labels = room_map))



# # new_df.loc[new_df["no_of_rooms"].isna(), "no_of_rooms"] = new_df["no_of_rooms"].map()


print(new_df.sample(10))