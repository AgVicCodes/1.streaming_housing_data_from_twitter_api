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

    queries = [
                'rent energy water tax Tv license groceries',
                'Birmingham rent energy water tax Tv license groceries', 
                'Coventry rent energy water tax Tv license groceries', 
                'Bristol rent energy water tax Tv license groceries', 
                'Loughborough rent energy water tax Tv license groceries',
                'Manchester rent energy water tax Tv license groceries',
                'Dundee rent energy water tax Tv license groceries',
                'Stoke rent energy water tax Tv license groceries',
                'Glasgow rent energy water tax Tv license groceries',
                'London rent energy water tax Tv license groceries',
                'Lincoln rent energy water tax Tv license groceries',
                'Swanley rent energy water tax Tv license groceries',
                'Northampton rent energy water tax Tv license groceries',
                'Durham rent energy water tax Tv license groceries',
                'Sheffield rent energy water tax Tv license groceries',
                'Leicester rent energy water tax Tv license groceries',
                'Scotland rent energy water tax Tv license groceries',
                'Luton rent energy water tax Tv license groceries',
                'Essex rent energy water tax Tv license groceries',
                'Bedford water tax groceries',
                'Aberdeen rent energy water tax Tv license groceries',
                'Bolton rent energy water tax Tv license groceries',
                'Canterbury rent energy water tax Tv license groceries',
                'Walsall rent energy water tax Tv license groceries',
                'Sunderland rent energy water tax Tv license groceries',
                'Leeds rent energy water tax Tv license groceries',
                'Westminster rent energy water tax Tv license groceries',
                'Ireland rent energy water tax Tv license groceries',
                'Wolverhampton rent energy water tax Tv license groceries',
                'Middlesbrough rent energy water tax Tv license groceries',
                'Derby rent energy water tax Tv license groceries',
                'Dartford rent energy water tax Tv license groceries',
                'Cambridgeshire rent energy water tax Tv license groceries',
                'Gloucester rent energy water tax Tv license groceries',
                'Gosport rent energy water tax Tv license groceries',
                'Peterborough rent energy water tax Tv license groceries',
                'Surrey rent energy water tax Tv license groceries'
            ]
    
    columns = ["text", "username", "timestamp"]
    data = []

    for query in queries:
        tweets = await client.search_tweet(query = query, product = "Top", count = 1000)

        for tweet in tweets:
            data.append([tweet.text, tweet.user.name, tweet.created_at])

            tweets_df = pd.DataFrame(data, columns = columns)

            path = "tweets_df.csv"

            # for i in range(len(queries)):    
            if os.path.exists(path):
                new_path = "tweets_df" + str(1) + ".csv"
                # new_path = "tweets_df" + str(i) + ".csv"
                tweets_df.to_csv(new_path, index = False)
            else:
                tweets_df.to_csv(path, index = False)


# Run the async function using the event loop
asyncio.run(main())
