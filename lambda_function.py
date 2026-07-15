import os
import json
import uuid
import boto3
import requests
from datetime import datetime, timezone

s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")
TFL_APP_KEY = os.getenv("TFL_APP_KEY")

TFL_URL = "https://api.tfl.gov.uk/Line/244/Arrivals"

def publish_delay_metrics(arrivals):
    for a in arrivals:
        time_to_station_sec = a.get("timeToStation")
        if time_to_station_sec is None:
            continue

        delay_minutes = time_to_station_sec / 60.0

        cloudwatch.put_metric_data(
            Namespace="TfL/Line244",
            MetricData=[
                {
                    "MetricName": "Delay_Minutes",
                    "Dimensions": [
                        {"Name": "StationName", "Value": a.get("stationName", "Unknown")},
                        {"Name": "LineId", "Value": a.get("lineId", "244")}
                    ],
                    "Timestamp": datetime.now(timezone.utc),
                    "Value": delay_minutes,
                    "Unit": "Minutes"
                }
            ]
        )

def publish_api_status_metric(status_code):
    cloudwatch.put_metric_data(
        Namespace="TfL/Line244",
        MetricData=[
            {
                "MetricName": "API_Success",
                "Value": 1 if status_code == 200 else 0,
                "Unit": "Count"
            },
            {
                "MetricName": "API_Error_5xx",
                "Value": 1 if status_code >= 500 else 0,
                "Unit": "Count"
            }
        ]
    )

def publish_arrivals_count(arrivals):
    counts = {}
    for a in arrivals:
        station = a.get("stationName", "Unknown")
        counts[station] = counts.get(station, 0) + 1

    metric_data = []
    for station, count in counts.items():
        metric_data.append({
            "MetricName": "Arrivals_Count",
            "Dimensions": [
                {"Name": "StationName", "Value": station},
                {"Name": "LineId", "Value": "244"}
            ],
            "Value": count,
            "Unit": "Count"
        })

    if metric_data:
        cloudwatch.put_metric_data(
            Namespace="TfL/Line244",
            MetricData=metric_data
        )

def lambda_handler(event, context):
    params = {"app_key": TFL_APP_KEY}

    response = requests.get(TFL_URL, params=params, timeout=10)
    status_code = response.status_code

    publish_api_status_metric(status_code)

    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "status_code": status_code,
        "data": None
    }

    if status_code == 200:
        data = response.json()
        payload["data"] = data

        publish_delay_metrics(data)
        publish_arrivals_count(data)
    else:
        payload["data"] = {"error": f"HTTP {status_code}"}

    key = f"line244/{datetime.now(timezone.utc).strftime('%Y/%m/%d/%H%M%S')}-{uuid.uuid4()}.json"

    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(payload),
        ContentType="application/json"
    )

    return {"statusCode": 200, "body": json.dumps({"s3_key": key})}
    print("CI/CD test")
