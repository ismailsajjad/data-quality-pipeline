import os
from pathlib import Path
from sourceconnection import get_engine
import pandas as pd
from sqlalchemy import create_engine

# CSV ka path file ki location se nikalte hain, taake PyCharm kahin se bhi run ho
CSV_PATH = Path(__file__).resolve().parent.parent / "src" / "customer.csv"
TABLE = "raw_customer"


def main():
    engine = create_engine(get_engine)

    df = pd.read_csv(CSV_PATH)
    df.columns = [c.strip().lower() for c in df.columns]

    print(f"CSV rows: {len(df)}")
    print(df.dtypes)

    df.to_sql(TABLE, engine, if_exists="replace", index=False)
    print(f"Loaded into {TABLE}")

if __name__ == "__main__":
    main()