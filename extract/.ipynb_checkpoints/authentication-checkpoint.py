import json
import twikit
import asyncio
import pandas as pd


queries = [
            'rent energy water tax Tv license groceries',
            'Aberdeen rent energy water tax Tv license groceries',
            'Bedford water tax groceries',
            'Birmingham rent energy water tax Tv license groceries', 
            'Bolton rent energy water tax Tv license groceries',
            'Bristol rent energy water tax Tv license groceries', 
            'Canterbury rent energy water tax Tv license groceries',
            'Cambridgeshire rent energy water tax Tv license groceries',
            'Coventry rent energy water tax Tv license groceries', 
            'Dartford rent energy water tax Tv license groceries',
            'Derby rent energy water tax Tv license groceries',
            'Dundee rent energy water tax Tv license groceries',
            'Durham rent energy water tax Tv license groceries',
            'Essex rent energy water tax Tv license groceries',
            'Glasgow rent energy water tax Tv license groceries',
            'Gloucester rent energy water tax Tv license groceries',
            'Gosport rent energy water tax Tv license groceries',
            'Ireland rent energy water tax Tv license groceries',
            'Leeds rent energy water tax Tv license groceries',
            'Leicester rent energy water tax Tv license groceries',
            'Lincoln rent energy water tax Tv license groceries',
            'London rent energy water tax Tv license groceries',
            'Loughborough rent energy water tax Tv license groceries',
            'Luton rent energy water tax Tv license groceries',
            'Manchester rent energy water tax Tv license groceries',
            'Middlesbrough rent energy water tax Tv license groceries',
            'Northampton rent energy water tax Tv license groceries',
            'Peterborough rent energy water tax Tv license groceries',
            'Scotland rent energy water tax Tv license groceries',
            'Sheffield rent energy water tax Tv license groceries',
            'Stoke rent energy water tax Tv license groceries',
            'Sunderland rent energy water tax Tv license groceries',
            'Surrey rent energy water tax Tv license groceries'
            'Swanley rent energy water tax Tv license groceries',
            'Walsall rent energy water tax Tv license groceries',
            'Westminster rent energy water tax Tv license groceries',
            'Wolverhampton rent energy water tax Tv license groceries'
        ]

columns = ["text", "username", "timestamp", "location"]


def extract_tweets():
    """ 
    Twikit tweet extractor.

    This function uses the keys in the keys.json file
    to authenticate the twikit api and uses the search_tweet 
    attribute to extract tweets through the API.
    Parameters

    """
    with open('keys.json') as file:
        keys = json.load(file)

    # Creating a client instance
    client = twikit.client.client.Client(language = "en-US")

    # Defining an async function to handle the async login
    async def main():
        try:
            await client.login(
                auth_info_1 = keys['Email'],
                auth_info_2 = keys['Username'],
                password = keys['Password']
            )
        except Exception as e:
            print(f"Login failed: {e}")
            return

        try:
            client.save_cookies('cookies.json')
        
            with open('cookies.json', 'r', encoding='utf-8') as f:
                client.set_cookies(json.load(f))

            client.load_cookies('cookies.json')
        
        except Exception as e:
            print(f"Couldn't save cookies: {e}")



        for i, query in enumerate(queries):

            tweets = await client.search_tweet(query = query, product = "Top", count = 1000)

            for tweet in tweets:
                data = []
                data.append([tweet.text, tweet.user.name, tweet.created_at, tweet.coordinates])
                tweets_df = pd.DataFrame(data, columns = columns)

            tweets_df.to_parquet(f"data/parquet/tweets_df_{i}.parquet", index = False)

    # Running the async function using the event loop
    asyncio.run(main())

# extract_tweets()