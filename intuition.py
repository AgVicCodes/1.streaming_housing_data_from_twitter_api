""""
import twikit
import snscrape
import json

with open('keys.json') as file:
    keys = json.load(file)

# # Twitter API authentication
# auth = tweepy.OAuth1UserHandler(
#     keys["API Key"], keys["API Key Secret"], keys["Access Token"], keys["Access Token Secret"]
# )

# api = tweepy.API(auth)


# Define an async function to handle the tweet fetching
# async def fetch_tweets(client, query, max_tweets = 1000):
#     all_tweets = []
#     cursor = None

#     while len(all_tweets) < max_tweets:
#         tweets = await client.search_tweet(query = query, product = 'Latest', count = 100, cursor = cursor)
        
#         if not tweets:
#             break

#         all_tweets.extend(tweets)
#         cursor = tweets[-1].id  # Update max_id to the id of the last tweet

#         # Avoid hitting rate limits
#         await asyncio.sleep(1)

#     return all_tweets[:max_tweets]



"""



# path = "tweets_df.csv"

# # if os.path.exists(path):
# for i in range(5):
#     new_path = "tweets_df" + str(i) + ".csv"

#     print(new_path)
# tweets_df.to_csv({}, index = False)

# tweets_df.to_csv(path, index = False)

# import pandas as pd

# df = pd.read_csv("csv/tweets_df8.csv")
# # df = pd.read_csv("tweets_df36.csv")

# pd.set_option('display.max_rows', None)

# # assert df1.equals(df2) and df2.equals(df3)

# print(df)

# import pandas as pd
# # import fastparquet

# tweet_df = pd.read_parquet("parquet/twitter_df.parquet", engine = "fastparquet")

# print(tweet_df.head())




# import os

# file_path_old = "C:/Users/LENOVO/OneDrive - University of Hertfordshire/code/Projects/1_streaming_housing_data_from_twitter_api/csv"

# file_path = os.path.abspath('./csv')

# query = "'Nigerians in the UK''What is the average monthly bills in your city?'-filter:replies AND -filter:retweets"
# no_of_tweets = 50

# tweets = api.search_tweets(q = query, lang = "en", count_no = no_of_tweets, tweet_mode = "extended")

# attributes_container = [[tweet.user.name, tweet.created_at, tweet.full_text] for tweet in tweets]

# print(attributes_container)# from authentication import api
# file_list = os.listdir(file_path)
# csv_files = glob.glob("csv/*.{}".format("csv"))


# Use multiple keyword queries in your tweet search
# Queries can include cities, apartment type, taxes, clothing, and other special fields,
# Find a way to use airflow to automate this task extracting and savind the dataframes over a period of time.
# Finally, merge all the dataframes in the extract function
# Transform and load the dataframe
# Play around with it  

# names = "Clary has 2 friends, she spends time with them, Susan has 3 brothers and, John 4 sisters"

# sentiment = "Put vacation photos online (They were so cute) a few yrs ago. PC crashed, and now I forget the name of the site (I'm crying)."

# word = "12345abcde45"

# print(re.findall(r"(\w+)\s*\w*\s*(\d)\s(\w+)", names))

# print(re.findall(r"\(.+?\)", sentiment))

# print(re.findall(r"\d+?", word))

# In adherence to PEP8
# energy_regex = re.compile(r"""
#     [Ee]nergy            # Match 'Energy' or 'energy'
#     \s?                  # Optional whitespace
#     [:|-]*               # Optional colon or hyphen
#     \s?                  # Optional whitespace
#     £?                   # Optional pound sign
#     (?:\d{1,3},?)+       # Non-capturing group for digits with optional commas
# """, re.VERBOSE)

rent_regex = r"(?:[R|r]ent|[B|b]edroom|[B|b]d|[B|b]ed|[R|r]oom)\s?[:|-]*\s?£?(?:\d?,?\d+)+"
energy_regex = r"[E|e]nergy\s?[:|-]*\s?(?:\w+\s*)*£?(?:\d?,?\d+)+"
water_regex = r"[W|w]ater\s?[:|-]*\s?£?(?:\d?,?\d+)+"
tax_regex = r"[T|t]ax\s?[:|-]*\s?£?(?:\d?,?\d+)+"
groceries_regex = r"[G|g]roceries\s?[:|-]*\s?£?(?:\d?,?\d+)+"
clothing_regex = r"[C|c]lothing\s?[:|-]*\s?£?(?:\d?,?\d+)+"

# Selecting strings that match a digit,
# room_regex = r"\d\s?(?:[B|b]edroom|[B|b]d|[B|b]ed|[R|r]oom)"
# room_regex = r"\d\s?(?:[B|b]edroom|[B|b]d|[B|b]ed|[R|r]oom)\s?[:|-]*\s?£?(?:\d?,?\d+)+"
# county_regex = r"(?:Aberdeen|Bedford|Birmingham|Bolton|Bristol|Canterbury|Cambridgeshire|Coventry|Dartford|Derby|Dundee|Durham|Essex|Glasgow|Gloucester|Gosport|Ireland|Leeds|Leicester|Lincoln|London|Loughborough|Luton|Manchester|Middlesbrough|Northampton|Oxford|Peterborough|Scotland|Sheffield|Stoke|Sunderland|Surrey|Swanley|Walsall|Westminster|Wolverhampton)\b"

# rent = []
# counties = []
# apartment = []

# for index, row in df.iterrows():
#     rent_value = re.findall(rent_regex, row['text'])
#     rent.extend(rent_value)

# print(len(rent))
# print(rent[30:50])

# # Assuming df is your DataFrame and 'text' is the column with the text data

# # Your regular expression
# energy_regex = r"[Ee]nergy\s?[:|-]*\s?(?:\w+\s*)*£?(?:\d{1,3},?)+"

# # Apply the regular expression to the entire column at once using str.findall
# df['energy_matches'] = df['text'].apply(lambda x: re.findall(energy_regex, x))

# # If you need a flat list of all matches
# apartment = [item for sublist in df['energy_matches'] for item in sublist]

# # Print the results
# print(apartment)

# new_cols = ["username", "location", "rent", "energy", "water", "council_tax", "groceries", "clothing"]

# flat = [item for sublist in df["text"].apply(lambda x: re.findall(rent_regex, x)) for item in sublist]

# df["rent"] = pd.Series(flat)


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

# df["text"] = df["text"].str.replace('\n', ' ').str.replace('  ', ' ')

# df["location"] = df["text"].apply(lambda x: re.findall(county_regex, x)[0] if re.findall(county_regex, x) else np.NaN)

# df["rooms"] = df["text"].apply(lambda x: re.findall(room_regex, x)[0] if re.findall(room_regex, x) else np.NaN)

# df["rent"] = df["text"].apply(lambda x: re.findall(rent_regex, x)[0] if re.findall(rent_regex, x) else np.NaN)

# df["energy"] = df["text"].apply(lambda x: re.findall(energy_regex, x)[0] if re.findall(energy_regex, x) else np.NaN)

# df["water"] = df["text"].apply(lambda x: re.findall(water_regex, x)[0] if re.findall(water_regex, x) else np.NaN)

# df["council_tax"] = df["text"].apply(lambda x: re.findall(tax_regex, x)[0] if re.findall(tax_regex, x) else np.NaN)

# df["groceries"] = df["text"].apply(lambda x: re.findall(groceries_regex, x)[0] if re.findall(groceries_regex, x) else np.NaN)

# df["clothing"] = df["text"].apply(lambda x: re.findall(clothing_regex, x)[0] if re.findall(clothing_regex, x) else np.NaN)

# twitter_df = df[["location", "rooms", "rent", "energy", "water", "council_tax", "groceries", "clothing"]]

# new_df = twitter_df.copy()

# new_df = new_df.dropna(subset = "rent")

# new_df = new_df.drop_duplicates(subset = ["location", "rooms", "rent", "energy", "water", "council_tax", "groceries", "clothing"])

# print(f"location = {new_df['location'].isna().sum()}")
# print(f"rooms = {new_df['rooms'].isna().sum()}")
# print(f"rent = {new_df['rent'].isna().sum()}")
# print(f"energy = {new_df['energy'].isna().sum()}")
# print(f"water = {new_df['water'].isna().sum()}")
# print(f"council_tax = {new_df['council_tax'].isna().sum()}")
# print(f"groceries = {new_df['groceries'].isna().sum()}")
# print(f"clothing = {new_df['clothing'].isna().sum()}")

# print(new_df.shape)
# print(new_df.head())

# Selecting strings like Rent or rent with at most 1 or 0 space in front with
# at 0 or more hyphens or colons and at most one or more space, Euro sign and 
# Digits that represent the price 
# rent_regex = r"(?:[Rr]ent|[Bb]edroom|[Bb]d|[Bb]ed|[Rr]oom)\s?[:-]*\s?£?(\d?,?\d+)+"

# rent = []
# rent_extract = ""

# for i, row in df.iterrows():
#     rent_extract = re.findall(rent_regex, row["text"])
#     rent.extend(rent_extract)
#     # rent.append(rent_extract)

# print(len(rent))
# print(rent[:70])