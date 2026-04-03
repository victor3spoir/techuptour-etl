#!/usr/bin/env python3

import json
import os
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, desc, explode, when


def read_github_data(data_path: str = "data.json") -> pd.DataFrame:
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file not found: {data_path}")

    try:
        df = pd.read_json(data_path)
    except Exception:
        try:
            df = pd.read_csv(data_path, sep="\t", index_col=0)
        except Exception:
            with open(data_path, "r") as f:
                content = f.read()
            lines = [
                json.loads(line) for line in content.strip().split("\n") if line.strip()
            ]
            df = pd.DataFrame(lines)

    df = df.reset_index(drop=True)
    df = df.loc[:, ~df.columns.str.match(r"^\d+$")]

    if "topics" in df.columns:

        def parse_topics(x):
            if isinstance(x, list):
                return [str(t) for t in x if t]
            if isinstance(x, str):
                if x.startswith("["):
                    try:
                        parsed = eval(x)
                        return (
                            [str(t) for t in parsed if t]
                            if isinstance(parsed, list)
                            else []
                        )
                    except:
                        return []
            return []

        df["topics"] = df["topics"].apply(parse_topics)

    for col_name in [
        "description",
        "language",
        "updated_at",
        "owner",
        "full_name",
        "url",
    ]:
        if col_name in df.columns:
            df[col_name] = (
                df[col_name].astype(str).replace("None", "").replace("nan", "")
            )

    if "stars" in df.columns:
        df["stars"] = pd.to_numeric(df["stars"], errors="coerce").fillna(0).astype(int)

    print(f"Loaded {len(df)} GitHub projects")
    return df


def analyze_github_data(spark: SparkSession, pdf: pd.DataFrame) -> dict:
    df = spark.createDataFrame(pdf)
    results = {}

    top_starred = (
        df.filter((col("stars").isNotNull()) & (col("stars") > 0))
        .orderBy(desc("stars"))
        .limit(10)
        .select("full_name", "stars", "language", "url")
        .collect()
    )
    results["top_10_most_starred"] = [
        {
            "full_name": row.full_name,
            "stars": row.stars,
            "language": row.language,
            "url": row.url,
        }
        for row in top_starred
    ]

    top_recent = (
        df.filter((col("updated_at").isNotNull()) & (col("updated_at") != ""))
        .orderBy(desc("updated_at"))
        .limit(10)
        .select("full_name", "updated_at", "stars", "language", "url")
        .collect()
    )
    results["top_10_most_recent"] = [
        {
            "full_name": row.full_name,
            "updated_at": row.updated_at,
            "stars": row.stars,
            "language": row.language,
            "url": row.url,
        }
        for row in top_recent
    ]

    lang_dist = (
        df.filter((col("language").isNotNull()) & (col("language") != ""))
        .groupBy("language")
        .count()
        .orderBy(desc("count"))
        .collect()
    )
    results["distribution_by_language"] = [
        {"language": row.language, "count": row["count"]} for row in lang_dist
    ]

    top_topics = (
        df.select(explode("topics").alias("topic"))
        .filter((col("topic").isNotNull()) & (col("topic") != ""))
        .groupBy("topic")
        .count()
        .orderBy(desc("count"))
        .limit(10)
        .collect()
    )
    results["top_10_topics"] = [
        {"topic": row.topic, "count": row["count"]} for row in top_topics
    ]

    avg_stars_by_lang = (
        df.filter(
            (col("language").isNotNull())
            & (col("language") != "")
            & (col("stars").isNotNull())
            & (col("stars") > 0)
        )
        .groupBy("language")
        .agg(avg("stars").alias("avg_stars"), count("*").alias("count"))
        .orderBy(desc("avg_stars"))
        .collect()
    )
    results["avg_stars_by_language"] = [
        {
            "language": row.language,
            "avg_stars": round(row.avg_stars, 2),
            "project_count": row["count"],
        }
        for row in avg_stars_by_lang
    ]

    max_stars = df.filter(col("stars") > 0).agg({"stars": "max"}).collect()
    max_stars_val = max_stars[0][0] if max_stars and max_stars[0][0] else 1

    df_with_rank = (
        df.filter(
            (col("updated_at").isNotNull())
            & (col("updated_at") != "")
            & (col("stars").isNotNull())
            & (col("stars") > 0)
        )
        .withColumn(
            "recency_score",
            when(col("updated_at") >= datetime.now().isoformat()[:10], 100).otherwise(
                50
            ),
        )
        .withColumn(
            "stars_normalized",
            (col("stars") / max_stars_val * 100),
        )
        .withColumn(
            "trending_score",
            (col("recency_score") * 0.4 + col("stars_normalized") * 0.6),
        )
        .orderBy(desc("trending_score"))
        .limit(10)
    )

    trending = df_with_rank.select(
        "full_name", "stars", "language", "updated_at", "trending_score", "url"
    ).collect()
    results["top_10_trending"] = [
        {
            "full_name": row.full_name,
            "stars": row.stars,
            "language": row.language,
            "updated_at": row.updated_at,
            "trending_score": round(row.trending_score, 2),
            "url": row.url,
        }
        for row in trending
    ]

    prolific_owners = (
        df.filter((col("owner").isNotNull()) & (col("owner") != ""))
        .groupBy("owner")
        .agg(count("*").alias("project_count"), col("owner").alias("owner"))
        .orderBy(desc("project_count"))
        .limit(10)
        .collect()
    )
    results["top_10_prolific_owners"] = [
        {"owner": row.owner, "project_count": row.project_count}
        for row in prolific_owners
    ]

    results["summary_stats"] = {
        "total_projects": df.count(),
        "projects_with_stars": df.filter(
            (col("stars").isNotNull()) & (col("stars") > 0)
        ).count(),
        "projects_with_description": df.filter(
            (col("description").isNotNull()) & (col("description") != "")
        ).count(),
        "projects_by_language_count": len(results["distribution_by_language"]),
        "unique_owners": df.filter((col("owner").isNotNull()) & (col("owner") != ""))
        .select("owner")
        .distinct()
        .count(),
        "unique_topics": len(results["top_10_topics"]),
    }

    print(
        f"Analysis complete: {results['summary_stats']['total_projects']} projects processed"
    )
    return results


def save_results(results: dict, output_file: str = "output.json") -> None:
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"Results saved to {output_file}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("GitHub Projects Analysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        pdf = read_github_data("data.json")
        results = analyze_github_data(spark, pdf)
        save_results(results, "output.json")
        print("Pipeline completed")
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()
