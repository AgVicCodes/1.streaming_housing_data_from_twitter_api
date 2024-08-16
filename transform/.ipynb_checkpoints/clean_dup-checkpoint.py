def remove_na_dup(subset_df):
    
    df = subset_df.copy()
    
    subset = ["rent_cost", "energy_bill"]
    
    df = df.drop_duplicates(subset = "username", keep = "first")
    
    df = df.dropna(subset = subset, how = "all")

    df.to_parquet("data/parquet/removed_duplicate_df.parquet")

    return df
