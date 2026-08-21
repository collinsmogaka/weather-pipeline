import os
import json
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

CITIES = ["London", "New York", "Nairobi","Tokyo", "Paris", "Sydney", "Singapore"]

def fetch_weather_data(city: str) -> dict:
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

def save_raw_json(data: dict, city: str):
    now = datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    
    # Mirroring cloud object storage date partitioning
    dir_path = os.path.join("data", "raw", "weather", date_path)
    os.makedirs(dir_path, exist_ok=True)
    
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    filename = f"{city.lower().replace(' ', '_')}_{timestamp}.json"
    file_path = os.path.join(dir_path, filename)
    
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        
    print(f"Saved: {file_path}")

def run_extraction():
    if not API_KEY or API_KEY == "your_actual_api_key_here":
        raise ValueError("Missing valid OPENWEATHER_API_KEY in .env file")
        
    for city in CITIES:
        try:
            data = fetch_weather_data(city)
            save_raw_json(data, city)
        except Exception as e:
            print(f"Failed to fetch data for {city}: {e}")

if __name__ == "__main__":
    run_extraction()