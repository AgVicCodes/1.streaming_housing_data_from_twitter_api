import json
import twikit
import asyncio
import os
import pandas as pd

# Use multiple keyword queries in your tweet search
# Queries can include cities, apartment type, taxes, clothing, and other special fields,
# Find a way to use airflow to automate this task extracting and savind the dataframes over a period of time.
# Finally, merge all the dataframes in the extract function
# Transform and load the dataframe
# Play around with it  

with open('keys.json') as file:
    keys = json.load(file)

# Creating a client instance
client = twikit.client.client.Client(language = "en-US")

# Define an async function to handle the async login
async def main():
    await client.login(
        auth_info_1 = keys['Email'],
        auth_info_2 = keys['Username'],
        password = keys['Password']
    )

    client.save_cookies('cookies.json')

    with open('cookies.json', 'r', encoding='utf-8') as f:
        client.set_cookies(json.load(f))

    client.load_cookies('cookies.json')

    query = 'rent energy water council tax groceries'

    tweets = await client.search_tweets(query = query, product = "Latest", count = 1000)

    columns = ["text", "username", "timestamp"]
    data = []

    for tweet in tweets:
        data.append([tweet.text, tweet.user.name, tweet.created_at])

    tweets_df = pd.DataFrame(data, columns = columns)

    print(tweets_df.head())

    path = "tweets_df.csv"

    if os.path.exists(path):
        tweets_df.to_csv({"tweets_df.csv"}, index = False)
    
    tweets_df.to_csv(path, index = False)

# Run the async function using the event loop
asyncio.run(main())
