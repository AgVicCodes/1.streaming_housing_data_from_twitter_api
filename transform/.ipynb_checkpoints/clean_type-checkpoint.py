def string_to_float(new_df):

    new_df.loc[new_df["county"].isna(), "county"] = "United Kingdom"

    new_df.loc[:, "rent_cost"] = new_df["rent_cost"].str.replace(",", "")
    
    new_df.loc[:, "energy_bill"] = new_df["energy_bill"].str.replace(",", "")

    new_df["rent_cost"] = new_df["rent_cost"].astype("float")
    
    new_df["energy_bill"] = new_df["energy_bill"].astype("float")
    
    new_df["water"] = new_df["water"].astype("float")
    
    new_df["council_tax"] = new_df["council_tax"].astype("float")
    
    new_df["groceries"] = new_df["groceries"].astype("float")
    
    new_df["clothing"] = new_df["clothing"].astype("float")

    subset_df = new_df.copy()
    
    new_df.to_parquet("data/parquet/first_conversion_df.parquet")

    return subset_df