# producer.py
# -------------------------------------------------------------
# Filename: producer.py
# Author: Flores A., Ramirez A.
# Date: Nov 2024
# Description: 
#     This script defines a Kafka producer class that generates 
#     and sends sensor data to a specified Kafka topic at a 
#     defined interval. The sensor data is generated using the 
#     getSensorData function from the data_generator module.
# -------------------------------------------------------------

import json
from kafka import KafkaProducer
from time import sleep
import random
from data_generator import getSensorData

# Configuration
SERVER = 'lab9.alumchat.lol:9092'  # Address of the Kafka server
TOPIC = '21874'                     # Unique Kafka topic to which the data will be published

class Producer:
    """
    A class to represent a Kafka producer for sending sensor data.
    
    Attributes:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to which data will be sent.
    """

    def __init__(self, server=SERVER, topic=TOPIC):
        """
        Initializes the Kafka producer with the given server and topic.

        Parameters:
            server (str): The address of the Kafka server (default is SERVER).
            topic (str): The topic to publish data to (default is TOPIC).
        """
        # Initialize the Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=[server],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic

    def sendData(self):
        """
        Generates sensor data and sends it to the configured Kafka topic.
        
        This method retrieves the sensor data using the getSensorData 
        function and publishes it to the Kafka topic.
        """
        # Generate sensor data
        sensor_data = getSensorData()
        
        # Send the sensor data to the Kafka topic
        self.producer.send(self.topic, sensor_data)
        print(f"Sent data: {sensor_data}")

    def run(self):
        """
        Continuously sends sensor data at the specified interval.
        
        This method runs an infinite loop that calls sendData() and 
        sleeps for the specified interval. It can be interrupted 
        by a keyboard interrupt (Ctrl+C).

        Parameters:
            interval (int): Time interval (in seconds) between data sends 
                            (default is TIME_INTERVAL).
        """
        try:
            while True:
                self.sendData()    # Call the sendData method to send sensor data
                time_interval = random.randint(15, 30) 
                sleep(time_interval)    # Wait for the specified interval

        except KeyboardInterrupt:
            print("Stopping producer...")  # Graceful exit message on keyboard interrupt
            
        finally:
            self.producer.close()           # Ensure the producer is closed on exit