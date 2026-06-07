from airflow.decorators import dag, task
from datetime import datetime
import requests
import pandas as pd
import os


@dag(
    dag_id="weather_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["weather", "etl"],
)
def weather_etl_pipeline():

    @task
    def extract_weather() -> dict:
        """Fetch granular hourly forecast including apparent temperature"""
        print("Extracting multi-metric environmental forecast...")
        
        url = "https://api.open-meteo.com/v1/forecast"
        payload = {
            "latitude": 52.52, # Coordinates of good old Berlinciaga
            "longitude": 13.419998,
            "hourly": "temperature_2m,apparent_temperature",
            "timezone": "GMT",
            "forecast_days": 1
        }
        
        try:
            response = requests.get(url, params=payload, timeout=30)
            response.raise_for_status() 
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            print(f"Critical failure during extraction: {e}")
            raise
    
    @task
    def transform_weather(raw_data: dict) -> list:
        """Transform data: flatten metadata into every hourly record"""
        print("Executing precise schema mapping...")
        try:
            # 1. Extract the sovereign metadata
            lat = raw_data.get('latitude')
            lon = raw_data.get('longitude')
            tz = raw_data.get('timezone_abbreviation', 'GMT')
            elev = raw_data.get('elevation')
            
            # 2. Extract the arrays
            hourly = raw_data.get('hourly', {})
            times = hourly.get('time', [])
            temps = hourly.get('temperature_2m', [])
            feels_like = hourly.get('apparent_temperature', [])
            
            if not times:
                raise ValueError("Dataset empty.")

            structured_data_list = []
            
            # 3. Iterate and stamp the metadata onto every record
            for i in range(len(times)):
                temp_c = temps[i]
                feels_c = feels_like[i]
                
                # Math
                temp_f = round((temp_c * 9/5) + 32, 1)
                feels_f = round((feels_c * 9/5) + 32, 1)
                
                # Categorization baseline
                category = "Extremely Cold" if temp_c < 15 else ("Temperate" if temp_c < 25 else "Hot")
                
                # The exact schema you demanded
                structured_data_list.append({
                    "time": times[i],
                    "temperature_2m": temp_c,
                    "latitude": lat,
                    "longitude": lon,
                    "timezone": tz,
                    "elevation": elev,
                    "temperature_2m_Fahrenheit": temp_f,
                    "feels_like_celsius": feels_c,
                    "feels_like_fahrenheit": feels_f,
                    "temp_category": category
                })
            
            print(f"Schema enforced. {len(structured_data_list)} records prepped for load.")
            return structured_data_list
        except Exception as e:
            print(f"Transformation logic failed: {e}")
            raise

    @task
    def load_weather(data: dict) -> dict:
        """Secure weather data into physical storage (CSV and JSON)"""
        print("Initiating dual-format load sequence...")
        try:
            output_dir = "/opt/airflow/data"
            # Ensure the path exists before attempting to write
            os.makedirs(output_dir, exist_ok=True)

            date_str = datetime.now().strftime("%Y%m%d")
            base_path = f"{output_dir}/weather_{date_str}"
            
            csv_path = f"{base_path}.csv"
            json_path = f"{base_path}.json"

            # Utilize pandas to finalize the artifacts
            df = pd.DataFrame([data])
            df.to_csv(csv_path, index=False)
            df.to_json(json_path, orient="records", indent=4)

            print(f"Data secured. CSV: {csv_path} | JSON: {json_path}")
            return {"csv": csv_path, "json": json_path}
        except Exception as e:
            print(f"Load operation compromised: {e}")
            raise

    # Execution
    raw = extract_weather()
    transformed = transform_weather(raw)
    loaded = load_weather(transformed)


dag_instance = weather_etl_pipeline()
