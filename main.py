#!/usr/bin/env python3

# This program will read the data
# from the mqtt broker and store it
# in the postgres database.

import paho.mqtt.client as mqtt
import json
import psycopg2

# The mqtt broker address
broker_address = ""

# The topic to subscribe to
topic = ""

# The postgres database connection
conn = psycopg2.connect(
    dbname="",
    user="",
    password="",
    host="",
    port=""
)   

# The cursor object
cur = conn.cursor()

# The callback function to handle the message
def on_message(client, userdata, message):
    # Decode the message
    data = json.loads(message.payload.decode())
    print(data)

    # Insert the data into the database
    cur.execute(f"INSERT INTO raw_weather_data (time, data) VALUES (now(),'{json.dumps(data)}')")
    conn.commit()

# Create the mqtt client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_message = on_message

# Connect to the broker
client.connect(broker_address)
client.subscribe(topic)
client.loop_forever()

