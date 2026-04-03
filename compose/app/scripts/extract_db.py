import pandas as pd
from sqlalchemy import create_engine


def extract_data(
    db_url: str = "postgresql://postgres:postgres@extract-db:5432/postgres",
) -> pd.DataFrame:
    engine = create_engine(db_url)
    df = pd.read_sql_query("SELECT * FROM power_consumption", engine)
    return df


def save_data(df: pd.DataFrame, output_path: str = "consumption.json") -> None:
    df.to_json(output_path, orient="records", indent=4, date_format="iso")
    print(f"Extraction complete - {len(df)} records saved to {output_path}")


if __name__ == "__main__":
    df = extract_data()
    save_data(df)
