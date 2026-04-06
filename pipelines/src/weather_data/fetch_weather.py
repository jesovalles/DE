import os
import csv
import requests
import logging
from datetime import datetime
from pathlib import Path

API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY is not set")

DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data/weather_data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

UK_CITIES = [
    {"name": "London", "lat": 51.5074, "lon": -0.1278},
    {"name": "Birmingham", "lat": 52.4862, "lon": -1.8904},
    {"name": "Manchester", "lat": 53.4808, "lon": -2.2426},
    {"name": "Glasgow", "lat": 55.8642, "lon": -4.2518},
    {"name": "Liverpool", "lat": 53.4084, "lon": -2.9916},
    {"name": "Leeds", "lat": 53.8008, "lon": -1.5491},
    {"name": "Sheffield", "lat": 53.3811, "lon": -1.4701},
    {"name": "Bristol", "lat": 51.4545, "lon": -2.5879},
    {"name": "Newcastle", "lat": 54.9784, "lon": -1.6174},
    {"name": "Nottingham", "lat": 52.9548, "lon": -1.1581},
]


def fetch_weather_data():
    weather_data = []

    for city in UK_CITIES:
        url = (
            f"https://api.openweathermap.org/data/2.5/weather"
            f"?lat={city['lat']}&lon={city['lon']}&appid={API_KEY}&units=metric"
        )

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            weather = {
                "city": city["name"],
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "description": data["weather"][0]["description"],
                "date": datetime.utcnow().date(),
            }

            weather_data.append(weather)

        except Exception as e:
            logging.error(f"Error fetching {city['name']}: {e}")
            continue

    return weather_data


def save_to_csv(weather_data):
    if not weather_data:
        logging.warning("No weather data to save.")
        return

    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    filepath = DATA_DIR / f"weather_{date_str}.csv"

    fieldnames = ["city", "temperature", "humidity", "description", "date"]

    try:
        with open(filepath, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(weather_data)

        logging.info(f"CSV saved at {filepath}")

    except Exception as e:
        logging.error(f"Error saving CSV: {e}")