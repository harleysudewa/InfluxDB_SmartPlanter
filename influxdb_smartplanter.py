import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import paho.mqtt.client as mqtt

host = "localhost"
port = 1883
token = os.environ.get("INFLUXDB_TOKEN")
org = "PCU"
url = "http://192.168.1.150:8086"
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)
bucket="Smart Planter"

moisture_topic = "planter/moisture"
light_topic = "planter/light"
date_topic = "planter/respond/date"
humidity_topic = "planter/humidity"
temperature_topic = "planter/temperature"
pressure_topic = "planter/pressure"
rain_topic = "planter/rain"

def map_range(x, in_min, in_max, out_min, out_max):
  return (x - in_min) * (out_max - out_min) // (in_max - in_min) + out_min


def on_connect(client, userdata, flags, reason_code, properties):
    print("Connected to MQTT Broker, result code: " + str(reason_code))
    client.subscribe(moisture_topic)
    client.subscribe(light_topic)
    client.subscribe(date_topic)
    client.subscribe(humidity_topic)
    client.subscribe(temperature_topic)
    client.subscribe(pressure_topic)
    client.subscribe(rain_topic)
    print("subscribed to all topics")


def on_message(mqtt_client, userdata, msg):
    try:
        if msg.topic == moisture_topic:
            payload = int(msg.payload.decode("utf-8"))
            payload = map_range(payload, 0, 1024, 0, 100)
            measurement = "moisture"
        elif msg.topic == light_topic:
            payload = float(msg.payload.decode("utf-8"))
            measurement = "lux"
        elif msg.topic == date_topic:
            payload = int(msg.payload.decode("utf-8"))
            measurement = "date"
        elif msg.topic == humidity_topic:
            payload = float(msg.payload.decode("utf-8"))
            measurement = "humidity"
        elif msg.topic == temperature_topic:
            payload = float(msg.payload.decode("utf-8"))
            measurement = "temperature"
        elif msg.topic == pressure_topic:
            payload = float(msg.payload.decode("utf-8"))
            measurement = "pressure"
        elif msg.topic == rain_topic:
            payload = int(msg.payload.decode("utf-8"))
            measurement = "rain"
        topic = msg.topic
    except ValueError as e:
        print(e)
        return

    if payload == "" or measurement == "" or topic == "":
        print("data empty")
    point = (
        Point(topic)
        .field(measurement, payload)
    )
    write_api.write(bucket=bucket, org="PCU", record=point)
    print(f"written data {measurement}: {payload} to database")



if __name__ == "__main__":
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.on_connect = on_connect
    client.connect(host, port, 60)
    client.loop_start()

    while True:
        continue