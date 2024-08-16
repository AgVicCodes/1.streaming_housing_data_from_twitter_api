import numpy as np
import pandas as pd

def populate_df(df):
    
    pseudo_rent = np.round(np.random.normal(df["rent_cost"].mean(), 200, df["rent_cost"].isnull().sum()), -2)

    pseudo_energy = np.round(np.random.normal(df["energy_bill"].mean(), 50, df["energy_bill"].isnull().sum()), -1)

    pseudo_water = np.round(np.random.normal(df["water"].mean(), 5, df["water"].isnull().sum()), -1)

    pseudo_tax = np.round(np.random.normal(df["council_tax"].mean(), 10, df["council_tax"].isnull().sum()), -1)

    pseudo_groceries = np.round(np.random.normal(df["groceries"].mean(), 2, df["groceries"].isnull().sum()), -1)

    pseudo_clothing = np.round(np.random.normal(df["clothing"].mean(), 10, df["clothing"].isnull().sum()), -1)



    df.loc[df["rent_cost"].isna(), "rent_cost"] = pseudo_rent

    df.loc[df["energy_bill"].isna(), "energy_bill"] = pseudo_energy

    df.loc[df["water"].isna(), "water"] = pseudo_water

    df.loc[df["council_tax"].isna(), "council_tax"] = pseudo_tax

    df.loc[df["groceries"].isna(), "groceries"] = pseudo_groceries

    df.loc[df["clothing"].isna(), "clothing"] = pseudo_clothing



    rent_range = [0, 600, 1000, np.inf]

    room_map = ["1", "2", "3"]

    df.loc[df["no_of_rooms"].isna(), "no_of_rooms"] = pd.cut(df["rent_cost"], bins = rent_range, labels = room_map)

    df.loc[:, "no_of_rooms"] = df["no_of_rooms"].astype("category")

    df.to_parquet("data/parquet/replaced_na_df.parquet")

    return df