import numpy as np
import pandas as pd
import re

# Selecting strings like Rent or rent with at most 1 or 0 space in front with
# at 0 or more hyphens or colons and at most one or more space, Euro sign and 
# Digits that represent the price 
rent_regex = r"(?:[R|r]ent|[B|b]edroom|[B|b]d|[B|b]ed|[R|r]oom)\s?[:|-]*\s?£?(\d?,?\d+)+"
energy_regex = r"[E|e]nergy\s?[:|-]*(?:\D+\s?)?\s?£?(\d?,?\d+)+"
water_regex = r"[W|w]ater\s?[:|-]*\s?£?(\d?,?\d+)+"
tax_regex = r"[C|c]ouncil\s?[T|t]ax\s?[:|-]*\s?£?(\d?,?\d+)+"
groceries_regex = r"[G|g]roceries\s?[:|-]*\s?£?(\d?,?\d+)+"
clothing_regex = r"[C|c]lothing\s?[:|-]*\s?£?(\d?,?\d+)+"

# Selecting strings that match a digit,
room_regex = r"(\d)\s?(?:[B|b]edroom|[B|b]d|[B|b]ed|[R|r]oom)"
county_regex = r"(?:Aberdeen|Bedford|Birmingham|Bolton|Bristol|Canterbury|Cambridgeshire|Coventry|Dartford|Derby|Dundee|Durham|Essex|Glasgow|Gloucester|Gosport|Ireland|Leeds|Leicester|Lincoln|London|Loughborough|Luton|Manchester|Middlesbrough|Northampton|Oxford|Peterborough|Scotland|Sheffield|Stoke|Sunderland|Surrey|Swanley|Walsall|Westminster|Wolverhampton)\b"

df = pd.read_parquet("parquet/twitter_df.parquet", engine = "fastparquet")

df["text"] = df["text"].str.replace('\n', ' ').str.replace('  ', ' ')

df["location"] = df["text"].apply(lambda x: re.findall(county_regex, x)[0] if re.findall(county_regex, x) else np.NaN)

df["rooms"] = df["text"].apply(lambda x: re.findall(room_regex, x)[0] if re.findall(room_regex, x) else np.NaN)

df["rent"] = df["text"].apply(lambda x: re.findall(rent_regex, x)[0] if re.findall(rent_regex, x) else np.NaN)

df["energy"] = df["text"].apply(lambda x: re.findall(energy_regex, x)[0] if re.findall(energy_regex, x) else np.NaN)

df["water"] = df["text"].apply(lambda x: re.findall(water_regex, x)[0] if re.findall(water_regex, x) else np.NaN)

df["council_tax"] = df["text"].apply(lambda x: re.findall(tax_regex, x)[0] if re.findall(tax_regex, x) else np.NaN)

df["groceries"] = df["text"].apply(lambda x: re.findall(groceries_regex, x)[0] if re.findall(groceries_regex, x) else np.NaN)

df["clothing"] = df["text"].apply(lambda x: re.findall(clothing_regex, x)[0] if re.findall(clothing_regex, x) else np.NaN)

twitter_df = df[["location", "rooms", "rent", "energy", "water", "council_tax", "groceries", "clothing"]]

new_df = twitter_df.copy()

new_df = new_df.dropna(subset = "rent")

new_df = new_df.drop_duplicates(subset = ["location", "rooms", "rent", "energy", "water", "council_tax", "groceries", "clothing"])

print(f"location = {new_df['location'].isna().sum()}")
print(f"rooms = {new_df['rooms'].isna().sum()}")
print(f"rent = {new_df['rent'].isna().sum()}")
print(f"energy = {new_df['energy'].isna().sum()}")
print(f"water = {new_df['water'].isna().sum()}")
print(f"council_tax = {new_df['council_tax'].isna().sum()}")
print(f"groceries = {new_df['groceries'].isna().sum()}")
print(f"clothing = {new_df['clothing'].isna().sum()}")

print(new_df.shape)
print(new_df.head())

# text = twitter_df.text

# Add Bedroom to the rent regex pattern to find the ones with bedrooms inside


# rent = []
# counties = []
# apartment = []

# for index, row in df.iterrows():
#     # rent_value = re.findall(room_regex, row['text'])
#     # rent.extend(rent_value)
#     # matches = re.findall(county_regex, row['text'])
#     # counties.extend(matches)
#     rooms = re.findall(energy_regex, row['text'])
#     apartment.extend(rooms)

# # print(len(rent))
# # print(rent[30:50])

# # print(len(counties))
# # print(counties[:30])

# print(len(apartment))
# print(apartment[100:150])