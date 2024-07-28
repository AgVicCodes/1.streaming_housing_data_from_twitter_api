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