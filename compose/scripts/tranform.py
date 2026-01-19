from pyspark.sql import SparkSession
import argparse


def main(input_path, output_path):
    spark = SparkSession.builder.appName("ETL Transform").getOrCreate()

    # Read extracted data (example CSV, adjust to your format)
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Perform transformations (placeholder)
    transformed_df = df  # TODO: add your transformation logic here

    # Write output (parquet, partitioned for example)
    transformed_df.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input path")
    parser.add_argument("--output", required=True, help="Output path")
    args = parser.parse_args()

    main(args.input, args.output)
