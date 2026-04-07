import os
import requests
import boto3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key

BUCKET = os.environ["S3_BUCKET"]
TABLE  = os.environ.get("DYNAMO_TABLE", "weather-tracking")
REGION = os.environ.get("AWS_REGION", "us-east-1")

LAT, LON = 38.0293, -78.4767
LOCATION = "Charlottesville, VA"

def fetch_weather():
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={LAT}&longitude={LON}"
        "&current=temperature_2m,wind_speed_10m,precipitation,cloud_cover"
        "&temperature_unit=fahrenheit&wind_speed_unit=mph"
    )
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    c = r.json()["current"]
    return {
        "temperature_f": str(c["temperature_2m"]),
        "wind_mph":      str(c["wind_speed_10m"]),
        "precip_mm":     str(c["precipitation"]),
        "cloud_pct":     str(c["cloud_cover"]),
    }

def write_dynamo(weather):
    db = boto3.resource("dynamodb", region_name=REGION)
    table = db.Table(TABLE)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    item = {"location": LOCATION, "timestamp": ts, **weather}
    table.put_item(Item=item)
    print(f"Wrote: {item}")

def read_history():
    db = boto3.resource("dynamodb", region_name=REGION)
    table = db.Table(TABLE)
    resp = table.query(
        KeyConditionExpression=Key("location").eq(LOCATION)
    )
    return resp["Items"]

def make_plot(items):
    df = pd.DataFrame(items)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")
    df["temperature_f"] = df["temperature_f"].astype(float)
    df["wind_mph"]      = df["wind_mph"].astype(float)

    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    sns.lineplot(data=df, x="timestamp", y="temperature_f",
                 ax=axes[0], color="#E8593C", linewidth=2)
    axes[0].set_ylabel("Temperature (F)")
    axes[0].set_title(f"Weather in {LOCATION} — {len(df)} readings")

    sns.lineplot(data=df, x="timestamp", y="wind_mph",
                 ax=axes[1], color="#3B8BD4", linewidth=2)
    axes[1].set_ylabel("Wind speed (mph)")
    axes[1].set_xlabel("Time (UTC)")

    plt.tight_layout()
    plt.savefig("/tmp/plot.png", dpi=150)
    plt.close()

def upload_s3(local, key, content_type):
    s3 = boto3.client("s3", region_name=REGION)
    s3.upload_file(local, BUCKET, key, ExtraArgs={"ContentType": content_type})
    print(f"Uploaded {key}")

def export_csv(items):
    df = pd.DataFrame(items)
    df.to_csv("/tmp/data.csv", index=False)
    upload_s3("/tmp/data.csv", "data.csv", "text/csv")

if __name__ == "__main__":
    weather = fetch_weather()
    write_dynamo(weather)
    items = read_history()
    make_plot(items)
    upload_s3("/tmp/plot.png", "plot.png", "image/png")
    export_csv(items)
    print(f"Done. {len(items)} total readings.")
