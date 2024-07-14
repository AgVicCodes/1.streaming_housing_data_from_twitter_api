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

import pandas as pd

df = pd.read_csv("csv/tweets_df8.csv")
# df = pd.read_csv("tweets_df36.csv")

pd.set_option('display.max_rows', None)

# assert df1.equals(df2) and df2.equals(df3)

print(df)