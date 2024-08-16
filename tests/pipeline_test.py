import pytest
import pandas as pd
import numpy as np

@pytest.fixture
def df():
    df = pd.read_parquet("./data/parquet/clean_df.parquet", engine = "pyarrow")
    return df

def test_null_columns(df):
    assert df["county"].notna().all()
    assert df["rent_cost"].notna().all()
    assert df["energy_bill"].notna().all()

def test_data_type(df):
    assert df["county"].dtype == str or df["county"].dtype == "O"
    assert df["no_of_rooms"].dtype == "category" #or df["no_of_rooms"].dtype == "O"
    assert df["rent_cost"].dtype == "float" #or df["rent_cost"].dtype == "O"
    assert df["energy_bill"].dtype == "float" #or df["county"].dtype == "O"
    assert df["water"].dtype == "float" 
    assert df["council_tax"].dtype == "float" 
    assert df["groceries"].dtype == "float" 
    assert df["clothing"].dtype == "float" 

def test_rent_range(df):
    assert set(df["no_of_rooms"].unique()) == {"1", "2", "3"}
    assert df["rent_cost"].between(0.0,27000.0).all()