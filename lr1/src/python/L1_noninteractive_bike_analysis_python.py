
"""
Lab 1. Non-interactive PySpark solution for SF Bay Area Bike Share.

Examples:
  spark-submit --master yarn --deploy-mode cluster L1_noninteractive_bike_analysis_python_SUBMIT.py trips.csv stations.csv
  spark-submit L1_noninteractive_bike_analysis_python_SUBMIT.py file:///home/mapr/trips.csv file:///home/mapr/stations.csv --output /home/mapr/bike_lab_python_result_submit.txt
"""

import argparse
from datetime import datetime
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path

from pyspark import SparkConf, SparkContext


THREE_HOURS_SECONDS = 3 * 60 * 60


def parse_args():
    parser = argparse.ArgumentParser(description="Bike analysis in PySpark")
    parser.add_argument("trips_path", nargs="?", default="trips.csv", help="Path to trips.csv (HDFS/MapR-FS or file:///)")
    parser.add_argument("stations_path", nargs="?", default="stations.csv", help="Path to stations.csv (HDFS/MapR-FS or file:///)")
    parser.add_argument("--output", default="/home/mapr/bike_lab_python_result_submit.txt", help="Local text output path")
    return parser.parse_args()


def parse_time(value: str) -> datetime:
    return datetime.strptime(value.strip(), "%m/%d/%Y %H:%M")


def format_duration(total_seconds: int) -> str:
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return radius * c


def valid_zip(zip_code: str) -> bool:
    return zip_code is not None and zip_code.strip() != "" and zip_code.strip().lower() != "nil"


def main():
    args = parse_args()

    conf = SparkConf().setAppName("Lab1_BikeAnalysis_PySpark")
    conf.setIfMissing("spark.master", "local[*]")

    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    try:
        trip_data = sc.textFile(args.trips_path)
        station_data = sc.textFile(args.stations_path)

        trips_header = trip_data.first()
        stations_header = station_data.first()

        trips = trip_data.filter(lambda row: row != trips_header).map(lambda row: row.split(",", -1))
        stations = station_data.filter(lambda row: row != stations_header).map(lambda row: row.split(",", -1))

        trips_internal = trips.map(
            lambda row: {
                "trip_id": int(row[0].strip()),
                "duration": int(row[1].strip()),
                "start_date": parse_time(row[2].strip()),
                "start_station": row[3].strip(),
                "start_terminal": int(row[4].strip()),
                "end_date": parse_time(row[5].strip()),
                "end_station": row[6].strip(),
                "end_terminal": int(row[7].strip()),
                "bike_id": int(row[8].strip()),
                "subscription_type": row[9].strip(),
                "zip_code": row[10].strip(),
            }
        ).cache()

        stations_internal = stations.map(
            lambda row: {
                "station_id": int(row[0].strip()),
                "name": row[1].strip(),
                "lat": float(row[2].strip()),
                "lon": float(row[3].strip()),
                "dock_count": int(row[4].strip()),
                "city": row[5].strip(),
                "installation_date": row[6].strip(),
            }
        ).cache()

        trips_count = trips_internal.count()
        stations_count = stations_internal.count()

        max_bike_id, max_bike_total_duration = (
            trips_internal.map(lambda trip: (trip["bike_id"], int(trip["duration"])))
            .reduceByKey(lambda left, right: left + right)
            .takeOrdered(1, key=lambda item: -item[1])[0]
        )

        station_list = stations_internal.collect()
        max_distance = None
        for i in range(len(station_list)):
            for j in range(i + 1, len(station_list)):
                left = station_list[i]
                right = station_list[j]
                distance = haversine_km(left["lat"], left["lon"], right["lat"], right["lon"])
                candidate = (distance, left, right)
                if max_distance is None or candidate[0] > max_distance[0]:
                    max_distance = candidate

        max_bike_trips = sorted(
            trips_internal.filter(lambda trip: trip["bike_id"] == max_bike_id).collect(),
            key=lambda trip: (trip["start_date"], trip["trip_id"]),
        )

        bike_path = (
            "path not found"
            if not max_bike_trips
            else " -> ".join([max_bike_trips[0]["start_station"]] + [trip["end_station"] for trip in max_bike_trips])
        )

        bikes_count = trips_internal.map(lambda trip: trip["bike_id"]).distinct().count()

        heavy_users = (
            trips_internal.filter(lambda trip: valid_zip(trip["zip_code"]))
            .map(lambda trip: (trip["zip_code"].strip(), int(trip["duration"])))
            .reduceByKey(lambda left, right: left + right)
            .filter(lambda item: item[1] > THREE_HOURS_SECONDS)
            .sortBy(lambda item: -item[1])
            .collect()
        )

        result_lines = [
            "=" * 90,
            "LAB 1: San Francisco Bay Area Bike Share analysis (PySpark)",
            "=" * 90,
            f"Trips path: {args.trips_path}",
            f"Stations path: {args.stations_path}",
            f"Trips count: {trips_count}",
            f"Stations count: {stations_count}",
            "",
            "1. Bike with maximum runtime",
            f"bikeId = {max_bike_id}",
            f"Total duration = {format_duration(max_bike_total_duration)} ({max_bike_total_duration} sec)",
            "",
            "2. Maximum geodesic distance between stations",
            f"Station 1: {max_distance[1]['name']} (#{max_distance[1]['station_id']})",
            f"Station 2: {max_distance[2]['name']} (#{max_distance[2]['station_id']})",
            f"Distance = {max_distance[0]:.3f} km",
            "",
            "3. Path of the bike with maximum runtime",
            f"bikeId = {max_bike_id}",
            f"Number of trips = {len(max_bike_trips)}",
            f"Path = {bike_path}",
            "",
            "4. Number of bikes in the system",
            f"Number of bikes = {bikes_count}",
            "",
            "5. Users who spent more than 3 hours on trips",
            f"Users found = {len(heavy_users)}",
            "Note: trips.csv does not contain a separate userId, therefore zipCode is used as the available user identifier; empty values and nil are excluded.",
            "",
        ]

        for zip_code, total_sec in heavy_users:
            result_lines.append(f"zipCode = {zip_code}, total duration = {format_duration(total_sec)} ({total_sec} sec)")

        result_lines.append("")
        result_lines.append("Detailed trips for the bike with maximum runtime:")
        for trip in max_bike_trips:
            result_lines.append(
                f"{trip['start_date']} | {trip['start_station']} (#{trip['start_terminal']}) -> "
                f"{trip['end_station']} (#{trip['end_terminal']}) | {trip['duration']} sec"
            )
        result_lines.append("=" * 90)

        output_path = Path(args.output)
        output_path.write_text("\n".join(result_lines), encoding="utf-8")
        print("\n".join(result_lines))
        print(f"\nResult also saved to local file: {output_path}")
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
