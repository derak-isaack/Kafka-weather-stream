from quixstreams import Application
import json
from dotenv import load_dotenv, dotenv_values
import requests
import certifi 
from urllib.request import urlopen

api = dotenv_values(".env")
api_key = api["API_KEY"]

def get_json_data(url):
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode("utf-8")
    return json.loads(data)

def get_weather():
    # Get the weather data
    resp = requests.get("https://api.open-meteo.com/v1/forecast",
                        params={
                            "latitude": 52.52,
                            "longitude": 13.41,
                            "hourly": "temperature_2m",
                            "current_weather": "true",
                        })  
    weather = resp.json()
    return weather
# url = f"https://financialmodelingprep.com/api/v3/historical-price-full/AAPL?apikey={api_key}"
# print(get_json_data(url))

app = Application(
    broker_address="localhost:9092",
    loglevel="DEBUG",
)


def main():
    app = Application(
    broker_address="localhost:9092",
    loglevel="DEBUG"
)
    with app.get_producer() as producer:
        weather_berlin = get_weather()
        producer.produce(
            topic="weather_data",
            key="Berlin",
            value=json.dumps(weather_berlin),
        )
        
if __name__ == "main":
    main()