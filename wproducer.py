import json
import requests
from kafka import KafkaProducer
from time import sleep

# OpenWeatherMap API configuration
API_KEY = '5c6bfbf065ee61ff3248f2996f934334'

# List of cities including the new ones mapped from the stations with null city values
CITIES = [
    'Brighton', 
    'Canterbury', 
    'Glasgow', 
    'London', 
    'Cardiff', 
    'Manchester', 
    'Cambridge', 
    'Birmingham', 
    'Newcastle', 
    'Nottingham', 
    'Bristol', 
    'Southampton', 
    'Aylesbury',
    'Liverpool',  # For HILLSIDE, FAZAKERLEY, LIVERPOOL CENTRAL
    'Leeds',      # For HEADINGLEY, HELLIFIELD
    'Sheffield'   # For TINSLEY EAST JN
]

URL = 'https://api.openweathermap.org/data/2.5/weather'

# Kafka configuration
KAFKA_BROKER = '172.30.0.1:9092'
TOPIC = 'weather_data'

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather(city):
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }
    response = requests.get(URL, params=params)
    return response.json()

def produce_weather_data():
    while True:
        for city in CITIES:
            try:
                weather_data = fetch_weather(city)
                producer.send(TOPIC, weather_data)
                print(f'Produced weather data for {city}: {weather_data}')
            except Exception as e:
                print(f'Error producing weather data for {city}: {e}')
        sleep(600)  # Fetch and produce data every 10 minutes

if __name__ == "__main__":
    print("Starting weather data producer...")
    produce_weather_data()