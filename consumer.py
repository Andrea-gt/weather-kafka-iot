# -------------------------------------------------------------
# Filename: consumer.py
# Author: Flores A., Ramirez A.
# Date: Nov 2024
# Description: 
#     This script defines a Kafka consumer class that reads 
#     sensor data from a specified Kafka topic. The consumer 
#     continuously polls for new messages and processes them.
# -------------------------------------------------------------

import json
from kafka import KafkaConsumer

# Configuration
SERVER = 'lab9.alumchat.lol:9092'  # Address of the Kafka server
TOPIC = '21874'                     # Kafka topic from which to consume data

class Consumer:
    """
    A class to represent a Kafka consumer for receiving sensor data.
    
    Attributes:
        consumer (KafkaConsumer): The Kafka consumer instance.
        topic (str): The Kafka topic from which data will be consumed.
    """

    def __init__(self, server=SERVER, topic=TOPIC):
        """
        Initializes the Kafka consumer with the given server and topic.

        Parameters:
            server (str): The address of the Kafka server (default is SERVER).
            topic (str): The topic to consume data from (default is TOPIC).
        """
        # Initialize the Kafka consumer
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[server],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',  # Start reading at the earliest message
            enable_auto_commit=True,         # Automatically commit offsets
            group_id='sensor_data_group'     # Consumer group ID
        )
        self.topic = topic

    def consume_data(self):
        """
        Continuously consumes sensor data from the configured Kafka topic.
        
        This method processes each message received from the Kafka topic,
        printing the sensor data to the console.
        """
        print(f"Consuming messages from topic: {self.topic}")

        try:
            for message in self.consumer:
                # Each message is a record from the Kafka topic
                sensor_data = message.value
                print(f"Received data: {sensor_data}")  # Print the received sensor data

        except KeyboardInterrupt:
            print("Stopping consumer...")

        finally:
            self.consumer.close()  # Ensure the consumer is closed on exit
