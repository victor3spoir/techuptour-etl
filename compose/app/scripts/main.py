import pandas as pd
import json
import os

# The extracted file path from the previous task
# Kestra stores extracted files in the 'unzip_file' output
csv_path = "{{ outputs.unzip_file.files['Bank_Churn_Data_Dictionary.csv'] }}"

# Read CSV and convert to JSON
df = pd.read_csv(csv_path)
json_data = df.to_json(orient="records", indent=4)

# Write to the output file Kestra expects
with open("bank_customer.json", "w") as f:
    f.write(json_data)
