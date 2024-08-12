import numpy as np
import pandas as pd
import re


rent_regex = r"(?:[Rr]ent|[Bb]edroom|[Bb]d|[Bb]ed|[Rr]oom\s)\s?[:-]?\s?£?(\d{1,2}?,?\d{3,4})"

energy_regex = r"[Ee]nergy\s?[:-]*(?:\D+\s?)?\s?£?(\d?,?\d+)+"

water_regex = r"[Ww]ater\s?[:-]*\s?£?(\d{1,3})"

tax_regex = r"[Cc]ouncil\s?[Tt]ax\s?[:-]*\s?£?(\d{1,3})"

groceries_regex = r"[Gg]roceries\s?[:-]*\s?£?(\d{1,3})"

clothing_regex = r"[Cc]lothing\s?[:-]*\s?£?(\d{1,3})"

room_regex = r"(\d)\s?(?:[Bb]edroom|[Bb]d|[Bb]ed|[Rr]oom)"

county_regex = r"(?:Aberdeen|Bedford|Birmingham|Bolton|Bristol|Canterbury|Cambridgeshire|Coventry|Dartford|Derby|Dundee|Durham|Essex|Glasgow|Gloucester|Gosport|Ireland|Leeds|Leicester|Lincoln|London|Loughborough|Luton|Manchester|Middlesbrough|Northampton|Oxford|Peterborough|Scotland|Sheffield|Stoke|Sunderland|Surrey|Swanley|Walsall|Westminster|Wolverhampton)\b"

df = pd.read_parquet("parquet/twitter_df.parquet", engine = "pyarrow")

df["text"] = df["text"].str.replace("\n", " ")

df["county"] = df["text"].apply(lambda x: re.findall(county_regex, x)[0] if re.findall(county_regex, x) else np.nan)

df["rent_cost"] = df["text"].apply(lambda x: re.findall(rent_regex, x)[0] if re.findall(rent_regex, x) else np.nan)

df["no_of_rooms"] = df["text"].apply(lambda x: re.findall(room_regex, x)[0] if re.findall(room_regex, x) else np.nan)

df["energy_bill"] = df["text"].apply(lambda x: re.findall(energy_regex, x)[0] if re.findall(energy_regex, x) else np.nan)

df["council_tax"] = df["text"].apply(lambda x: re.findall(tax_regex, x)[0] if re.findall(tax_regex, x) else np.nan)

df["groceries"] = df["text"].apply(lambda x: re.findall(groceries_regex, x)[0] if re.findall(groceries_regex, x) else np.nan)

df["clothing"] = df["text"].apply(lambda x: re.findall(clothing_regex, x)[0] if re.findall(clothing_regex, x) else np.nan)  

df["water"] = df["text"].apply(lambda x: re.findall(water_regex, x)[0] if re.findall(water_regex, x) else np.nan)

new_df = df[["username", "county", "no_of_rooms", "rent_cost", "energy_bill", "water", "council_tax", "groceries", "clothing"]]