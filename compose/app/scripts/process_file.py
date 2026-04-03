#!/usr/bin/env python3

import json
import os

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, desc, when
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum


def read_customer_data(data_path: str = "data.json") -> pd.DataFrame:
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file not found: {data_path}")

    try:
        df = pd.read_json(data_path)
        print(f"Loaded {len(df)} customer records")
        return df
    except Exception as e:
        print(f"Error reading JSON: {e}")
        raise


def analyze_customer_data(spark: SparkSession, pdf: pd.DataFrame) -> dict:
    df = spark.createDataFrame(pdf)
    results = {}

    geo_dist = (
        df.filter(col("Geography").isNotNull())
        .groupBy("Geography")
        .agg(count("*").alias("count"))
        .orderBy(desc("count"))
        .collect()
    )
    results["distribution_by_geography"] = [
        {"geography": row.Geography, "count": row["count"]} for row in geo_dist
    ]

    gender_dist = (
        df.filter(col("Gender").isNotNull())
        .groupBy("Gender")
        .agg(count("*").alias("count"))
        .orderBy(desc("count"))
        .collect()
    )
    results["distribution_by_gender"] = [
        {"gender": row.Gender, "count": row["count"]} for row in gender_dist
    ]

    active_dist = (
        df.filter(col("IsActiveMember").isNotNull())
        .groupBy("IsActiveMember")
        .agg(count("*").alias("count"))
        .orderBy(desc("IsActiveMember"))
        .collect()
    )
    results["active_vs_inactive"] = [
        {"is_active": bool(row.IsActiveMember), "count": row["count"]}
        for row in active_dist
    ]

    high_credit = (
        df.filter(col("CreditScore") >= 700)
        .agg(count("*").alias("count"))
        .collect()[0][0]
    )
    results["high_credit_score_customers"] = {
        "threshold": 700,
        "count": high_credit,
        "percentage": round(high_credit / df.count() * 100, 2),
    }

    churn_dist = (
        df.filter(col("Exited").isNotNull())
        .groupBy("Exited")
        .agg(count("*").alias("count"))
        .orderBy(desc("Exited"))
        .collect()
    )
    churn_rate = df.filter(col("Exited") == 1).count() / df.count() * 100
    results["churn_analysis"] = {
        "distribution": [
            {"exited": bool(row.Exited), "count": row["count"]} for row in churn_dist
        ],
        "churn_rate": round(churn_rate, 2),
    }

    avg_salary_geo = (
        df.filter((col("Geography").isNotNull()) & (col("EstimatedSalary").isNotNull()))
        .groupBy("Geography")
        .agg(
            spark_round(avg("EstimatedSalary"), 2).alias("avg_salary"),
            count("*").alias("count"),
        )
        .orderBy(desc("avg_salary"))
        .collect()
    )
    results["avg_salary_by_geography"] = [
        {
            "geography": row.Geography,
            "avg_salary": row.avg_salary,
            "customer_count": row["count"],
        }
        for row in avg_salary_geo
    ]

    avg_age_geo = (
        df.filter((col("Geography").isNotNull()) & (col("Age").isNotNull()))
        .groupBy("Geography")
        .agg(spark_round(avg("Age"), 1).alias("avg_age"), count("*").alias("count"))
        .orderBy(desc("avg_age"))
        .collect()
    )
    results["avg_age_by_geography"] = [
        {
            "geography": row.Geography,
            "avg_age": row.avg_age,
            "customer_count": row["count"],
        }
        for row in avg_age_geo
    ]

    product_dist = (
        df.filter(col("NumOfProducts").isNotNull())
        .groupBy("NumOfProducts")
        .agg(count("*").alias("count"))
        .orderBy("NumOfProducts")
        .collect()
    )
    results["customer_segmentation_by_products"] = [
        {"num_products": row.NumOfProducts, "count": row["count"]}
        for row in product_dist
    ]

    balance_stats = (
        df.filter(col("Balance").isNotNull())
        .agg(
            spark_min("Balance").alias("min_balance"),
            spark_max("Balance").alias("max_balance"),
            spark_round(avg("Balance"), 2).alias("avg_balance"),
            count("*").alias("count"),
        )
        .collect()[0]
    )
    results["balance_distribution"] = {
        "min_balance": float(balance_stats.min_balance),
        "max_balance": float(balance_stats.max_balance),
        "avg_balance": float(balance_stats.avg_balance),
        "customers_with_balance": balance_stats["count"],
    }

    tenure_stats = (
        df.filter(col("Tenure").isNotNull())
        .agg(
            spark_min("Tenure").alias("min_tenure"),
            spark_max("Tenure").alias("max_tenure"),
            spark_round(avg("Tenure"), 1).alias("avg_tenure"),
            count("*").alias("count"),
        )
        .collect()[0]
    )
    results["tenure_analysis"] = {
        "min_tenure_years": int(tenure_stats.min_tenure),
        "max_tenure_years": int(tenure_stats.max_tenure),
        "avg_tenure_years": float(tenure_stats.avg_tenure),
        "total_customers": tenure_stats["count"],
    }

    churn_geo = (
        df.filter(col("Geography").isNotNull())
        .groupBy("Geography")
        .agg(
            count("*").alias("total"),
            spark_sum(when(col("Exited") == 1, 1).otherwise(0)).alias("churned"),
        )
        .orderBy(desc("churned"))
        .collect()
    )
    results["churn_by_geography"] = [
        {
            "geography": row.Geography,
            "total_customers": row.total,
            "churned": row.churned,
            "churn_rate": round(row.churned / row.total * 100, 2),
        }
        for row in churn_geo
    ]

    results["summary_stats"] = {
        "total_customers": df.count(),
        "countries": len(results["distribution_by_geography"]),
        "avg_age": round(
            df.filter(col("Age").isNotNull()).agg(avg("Age")).collect()[0][0], 1
        ),
        "avg_salary": round(
            df.filter(col("EstimatedSalary").isNotNull())
            .agg(avg("EstimatedSalary"))
            .collect()[0][0],
            2,
        ),
        "avg_credit_score": round(
            df.filter(col("CreditScore").isNotNull())
            .agg(avg("CreditScore"))
            .collect()[0][0],
            1,
        ),
    }

    print(
        f"Analysis complete: {results['summary_stats']['total_customers']} customers processed"
    )
    return results


def save_results(results: dict, output_file: str = "output.json") -> None:
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"Results saved to {output_file}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Customer Data Analysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        pdf = read_customer_data("data.json")
        results = analyze_customer_data(spark, pdf)
        save_results(results, "output.json")
        print("Pipeline completed")
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()
