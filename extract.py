# from authentication import api
import pandas as pd
# query = "'Nigerians in the UK''What is the average monthly bills in your city?'-filter:replies AND -filter:retweets"
# no_of_tweets = 50

# tweets = api.search_tweets(q = query, lang = "en", count_no = no_of_tweets, tweet_mode = "extended")

# attributes_container = [[tweet.user.name, tweet.created_at, tweet.full_text] for tweet in tweets]

# print(attributes_container)

tweets_df = pd.read_csv("csv/tweets_df5.csv")

print(tweets_df)