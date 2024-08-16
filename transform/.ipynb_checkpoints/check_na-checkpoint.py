def convert_type(df):
    
    df["no_of_rooms"] = df["no_of_rooms"].astype("category")
    
    df["rent_cost"] = df["rent_cost"].astype("float")
    
    df["energy_bill"] = df["energy_bill"].astype("float")
    
    df["water"] = df["water"].astype("float")
    
    df["council_tax"] = df["council_tax"].astype("float")
    
    df["groceries"] = df["groceries"].astype("float")
    
    df["clothing"] = df["clothing"].astype("float")

    clean_df = df[["county", "no_of_rooms", "rent_cost", "energy_bill", "water", "council_tax", "groceries", "clothing"]]

    clean_df = clean_df.reset_index()

    return clean_df