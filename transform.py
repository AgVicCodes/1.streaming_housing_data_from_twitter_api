import pandas as pd
import re


df = pd.read_parquet("parquet/twitter_df.parquet", engine = "fastparquet")

df["text"] = df["text"].str.replace('\n', ' ').str.replace('  ', ' ')

# print(twitter_df.head())

# new_cols = ["username", "location", "rent", "energy", "water", "council_tax", "groceries", "clothing"]

# # text = twitter_df.text

# county_regex = r"[Aberdeen|Bedford|Birmingham|Bolton|Bristol|Canterbury|Cambridgeshire|Coventry|Dartford|Derby|Dundee|Durham|Essex|Glasgow|Gloucester|Gosport|Ireland|Leeds|Leicester|Lincoln|London|Loughborough|Luton|Manchester|Middlesbrough|Northampton|Peterborough|Scotland|Sheffield|Stoke|Sunderland|Surrey|Swanley|Walsall|Westminster|'Wolverhampton']"

# pattern = '|'.join(map(re.escape, counties))

# matches = county.findall(counties)

# county = re.compile(pattern)

# Selecting strings like Rent or rent with at most 1 or 0 space in front with
# at 0 or more hyphens or colons and at most one or more space, Euro sign and 
# Digits that represent the price 
rent_regex = r"[R|r]ent\s?[:|-]*\s?£?(?:\d?,?\d+)+"
# energy_regex = r"[E|e]nergy\s?[:|-]*\s?(?:\w+\s*)*£?(?:\d?,?\d+)+"
# water_regex = r"[W|w]ater\s?[:|-]*\s?£?(?:\d?,?\d+)+"
# tax_regex = r"[T|t]ax\s?[:|-]*\s?£?(?:\d?,?\d+)+"
# groceries_regex = r"[G|g]roceries\s?[:|-]*\s?£?(?:\d?,?\d+)+"
# clothing_regex = r"[C|c]lothing\s?[:|-]*\s?£?(?:\d?,?\d+)+"

# Selecting strings that match a digit,
room_regex = r"\d\s?(?:[B|b]edroom|[B|b]d|[B|b]ed|[R|r]oom)"
# room_regex = r"\d\s?(?:[B|b]edroom|[B|b]d|[B|b]ed|[R|r]oom)\s?[:|-]*\s?£?(?:\d?,?\d+)+"
# _regex = r"[R|r]ent\s?[:|-]*\s?£?(?:\d?,?\d+)+"
# _regex = r"[R|r]ent\s?[:|-]*\s?£?(?:\d?,?\d+)+"
# county_regex = r"(?:Aberdeen|Bedford|Birmingham|Bolton|Bristol|Canterbury|Cambridgeshire|Coventry|Dartford|Derby|Dundee|Durham|Essex|Glasgow|Gloucester|Gosport|Ireland|Leeds|Leicester|Lincoln|London|Loughborough|Luton|Manchester|Middlesbrough|Northampton|Oxford|Peterborough|Scotland|Sheffield|Stoke|Sunderland|Surrey|Swanley|Walsall|Westminster|Wolverhampton)\b"


# regex = r"[R|r]ent\s*(?:\w*\s*)*\."

# regex1 = r"(?<!brown\s)(cat|dog)" # Look behind negative
# regex1 = r"(?<=brown\s)(cat|dog)" # Look behing
# regex2 = r"\w+\.txt(?=\stransferred)" # Look ahead
# regex2 = r"\w+\.txt(?!\stransferred)" # Look ahead negative

rent = []
counties = []
apartment = []

for index, row in df.iterrows():
    rent_value = re.findall(rent_regex, row['text'])
    rent.extend(rent_value)
    # matches = re.findall(county_regex, row['text'])
    # counties.extend(matches)
    # rooms = re.findall(room_regex, row['text'])
    # apartment.extend(rooms)

print(len(rent))
print(rent[:30])

# print(len(counties))
# print(counties[:30])

# print(len(apartment))
# print(apartment[:10])