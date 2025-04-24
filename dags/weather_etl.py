from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2
import os


# Set your API key and city
API_KEY = os.getenv("OPENWEATHER_API_KEY")
#list 10 cities in UK you want data about
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
    {"name": "Nottingham", "lat": 52.9548, "lon": -1.1581}
]


# Fetch weather data from OpenWeatherMap
def fetch_weather(lat, lon, city_name):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
    
    data = response.json()
    
    if 'main' not in data or 'weather' not in data:
        raise Exception(f"Missing expected data in API response: {data}")
    
    weather = {
        "city": city_name,
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "description": data["weather"][0]["description"],
        "date": datetime.utcnow().date()
    }
    return weather



def store_weather():
    import logging

    conn = psycopg2.connect(
        host="postgres",
        database="weatherdb",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    for city in UK_CITIES:
        try:
            weather = fetch_weather(city["lat"], city["lon"], city["name"])
            logging.info(f"Fetched weather for {city['name']}: {weather}")

            insert_query = """
            INSERT INTO weather (city, temperature, humidity, weather_description, date)
            VALUES (%s, %s, %s, %s, %s)
            """
            cur.execute(insert_query, (
                weather["city"],
                weather["temperature"],
                weather["humidity"],
                weather["description"],
                weather["date"]
            ))

            logging.info(f"Inserted weather for {city['name']} successfully.")
        except Exception as e:
            logging.error(f"Error with {city['name']}: {e}")

    conn.commit()
    cur.close()
    conn.close()


# Airflow DAG setup
default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="weather_etl",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
) as dag:
    
    store_weather_task = PythonOperator(
        task_id="store_weather",
        python_callable=store_weather
    )

    store_weather_task
