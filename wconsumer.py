import json
import datetime
from kafka import KafkaConsumer
import snowflake.connector

# Snowflake connection details
snowflake_conn = snowflake.connector.connect(
    user='HarryPotter',
    password='Advvamsi@37870',
    account='WISMGCK-TW41160',
    warehouse='rail_weather',
    database='rail_data',
    schema='public'
)

def insert_weather_data_to_snowflake(data):
    try:
        cursor = snowflake_conn.cursor()
        sql = """
        INSERT INTO weather_data (city, temperature, humidity, pressure, visibility, wind_speed, wind_direction, sunrise, sunset, weather_description, cloudiness, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """
        sunrise_unix = data['sys']['sunrise']
        sunset_unix = data['sys']['sunset']
        
        # Convert UNIX timestamps to datetime objects
        #sunrise_datetime = datetime.datetime.utcfromtimestamp(sunrise_unix) --> deprecated
        # sunset_datetime = datetime.datetime.utcfromtimestamp(sunset_unix)  --> deprecated
        sunrise_datetime = datetime.datetime.fromtimestamp(
                                sunrise_unix, tz=datetime.timezone.utc
                                )
        sunset_datetime = datetime.datetime.fromtimestamp(
                                        sunset_unix, tz=datetime.timezone.utc
                                        )
        
        # Convert datetime objects to Snowflake TIMESTAMP_NTZ(9) format
        sunrise = sunrise_datetime.strftime('%Y-%m-%d %H:%M:%S')
        sunset = sunset_datetime.strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute(sql, (
            data['name'],
            data['main']['temp'],
            data['main']['humidity'],
            data['main']['pressure'],
            data['visibility'],
            data['wind']['speed'],
            data['wind']['deg'],
            sunrise,
            sunset,
            data['weather'][0]['description'],
            data['clouds']['all']
        ))
        snowflake_conn.commit()
        cursor.close()
        print(f"Inserted weather data for {data['name']} into Snowflake")
    except Exception as e:
        print(f"Error inserting data into Snowflake: {e}")

def consume_weather_data():
    consumer = KafkaConsumer('weather_data',
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest',
                             group_id='weather_group',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    print("Starting weather data consumer...")
    for message in consumer:
        weather_data = message.value
        insert_weather_data_to_snowflake(weather_data)

if __name__ == "__main__":
    consume_weather_data()