import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:postgres@extract-db:5432/postgres")

# SQL to DataFrame
df = pd.read_sql_query("SELECT * FROM power_consumption", engine)

# Output to JSON with ISO date format
df.to_json("consumption.json", orient="records", indent=4, date_format="iso")
print("Extraction complete.")
