import json

import pandas as pd


def read_data(data_path: str = "data.json") -> pd.DataFrame:
    try:
        with open(data_path, "r") as f:
            content = f.read()
            lines = [json.loads(line) for line in content.strip().split("\n") if line]
            df = pd.DataFrame(lines)
    except (json.JSONDecodeError, ValueError):
        try:
            df = pd.read_json(data_path)
        except Exception as e:
            print(f"Error reading {data_path}: {e}")
            raise
    return df


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["hour"] = df["datetime"].dt.hour
    df["month"] = df["datetime"].dt.month
    df["total_consumption"] = (
        df["power_consumption_zone1"]
        + df["power_consumption_zone2"]
        + df["power_consumption_zone3"]
    )
    return df


def analyze_data(df: pd.DataFrame) -> dict:
    analyses = {}
    total_consumption = float(df["total_consumption"].sum())

    analyses["total_energy_per_zone"] = {
        "zone1_kwh": round(float(df["power_consumption_zone1"].sum()), 2),
        "zone2_kwh": round(float(df["power_consumption_zone2"].sum()), 2),
        "zone3_kwh": round(float(df["power_consumption_zone3"].sum()), 2),
    }

    analyses["total_energy_all_zones"] = round(total_consumption, 2)

    analyses["average_consumption_per_zone"] = {
        "zone1_kwh": round(float(df["power_consumption_zone1"].mean()), 2),
        "zone2_kwh": round(float(df["power_consumption_zone2"].mean()), 2),
        "zone3_kwh": round(float(df["power_consumption_zone3"].mean()), 2),
    }

    if total_consumption > 0:
        analyses["energy_distribution_percentage"] = {
            "zone1_percent": round(
                (float(df["power_consumption_zone1"].sum()) / total_consumption) * 100,
                2,
            ),
            "zone2_percent": round(
                (float(df["power_consumption_zone2"].sum()) / total_consumption) * 100,
                2,
            ),
            "zone3_percent": round(
                (float(df["power_consumption_zone3"].sum()) / total_consumption) * 100,
                2,
            ),
        }

    analyses["highest_temperature_all_time"] = float(df["temperature"].max())
    analyses["lowest_temperature_all_time"] = float(df["temperature"].min())

    max_temp_by_month = df.groupby("month")["temperature"].max()
    analyses["max_temperature_by_month"] = {
        f"month_{int(month)}": float(temp) for month, temp in max_temp_by_month.items()
    }

    min_temp_by_month = df.groupby("month")["temperature"].min()
    analyses["min_temperature_by_month"] = {
        f"month_{int(month)}": float(temp) for month, temp in min_temp_by_month.items()
    }

    analyses["temperature_statistics"] = {
        "average_celsius": round(float(df["temperature"].mean()), 2),
        "max_celsius": float(df["temperature"].max()),
        "min_celsius": float(df["temperature"].min()),
    }

    analyses["humidity_statistics"] = {
        "average_percent": round(float(df["humidity"].mean()), 2),
        "max_percent": float(df["humidity"].max()),
        "min_percent": float(df["humidity"].min()),
    }

    analyses["wind_speed_statistics"] = {
        "average_mps": round(float(df["wind_speed"].mean()), 3),
        "max_mps": round(float(df["wind_speed"].max()), 3),
        "min_mps": round(float(df["wind_speed"].min()), 3),
    }

    hourly_consumption = df.groupby("hour")["total_consumption"].mean()
    analyses["average_consumption_by_hour"] = {
        f"hour_{int(hour)}": round(float(consumption), 2)
        for hour, consumption in hourly_consumption.items()
    }

    peak_hour_idx = hourly_consumption.idxmax()
    analyses["peak_consumption_hour"] = {
        "hour": int(peak_hour_idx),
        "average_kwh": round(float(hourly_consumption[peak_hour_idx]), 2),
    }

    monthly_consumption = df.groupby("month")["total_consumption"].mean()
    analyses["average_consumption_by_month"] = {
        f"month_{int(month)}": round(float(consumption), 2)
        for month, consumption in monthly_consumption.items()
    }

    analyses["summary"] = {
        "total_records": int(len(df)),
        "months_covered": int(df["month"].nunique()),
        "date_range_start": str(df["datetime"].min()),
        "date_range_end": str(df["datetime"].max()),
    }

    return analyses


def save_results(analyses: dict, output_path: str = "output.json") -> None:
    with open(output_path, "w") as f:
        json.dump(analyses, f, indent=2)
    print(f"Analysis completed - results saved to {output_path}")


if __name__ == "__main__":
    df = read_data("data.json")
    df = prepare_data(df)
    analyses = analyze_data(df)
    save_results(analyses)
