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
import random
from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError
from data_generator import getSensorData

# Configuration
SERVER = 'lab9.alumchat.lol:9092'  # Address of the Kafka server
TOPIC = '21874'  # Unique Kafka topic to which the data will be published
MIN_INTERVAL = 15  # Minimum time between sends in seconds
MAX_INTERVAL = 30  # Maximum time between sends in seconds

class Producer:
    """
    A class to represent a Kafka producer for sending sensor data.
    
    Attributes:
        producer (KafkaProducer): The Kafka producer instance
        topic (str): The Kafka topic to which data will be sent
    """
    
    def __init__(self, server=SERVER, topic=TOPIC):
        """
        Initializes the Kafka producer with the given server and topic.
        
        Parameters:
            server (str): The address of the Kafka server (default is SERVER)
            topic (str): The topic to publish data to (default is TOPIC)
        
        Raises:
            KafkaError: If connection to Kafka server fails
        """
        try:
            # Initialize the Kafka producer with additional configuration
            self.producer = KafkaProducer(
                bootstrap_servers=[server],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas to acknowledge
                request_timeout_ms=5000  # Timeout for requests
            )
            self.topic = topic
            print(f"Successfully connected to Kafka server at {server}")

        except KafkaError as e:
            print(f"Failed to connect to Kafka server: {e}")
            raise

    def sendData(self):
        """
        Generates sensor data and sends it to the configured Kafka topic.
        
        Returns:
            bool: True if send was successful, False otherwise
        """
        try:
            # Generate sensor data
            sensor_data = getSensorData()
            # Send the sensor data to the Kafka topic
            future = self.producer.send(self.topic, sensor_data)
            # Wait for the send to complete and check for errors
            record_metadata = future.get(timeout=10)
            
            print(f"Sent data: {sensor_data}")
            print(f"Message delivered to topic {record_metadata.topic} "
                  f"partition {record_metadata.partition} "
                  f"offset {record_metadata.offset}")
            return True
            
        except Exception as e:
            print(f"Error sending data: {e}")
            return False

    def run(self, min_interval=MIN_INTERVAL, max_interval=MAX_INTERVAL):
        """
        Continuously sends sensor data at random intervals.
        
        Parameters:
            min_interval (int): Minimum time between sends in seconds
            max_interval (int): Maximum time between sends in seconds
        """

        print(f"\nStarting producer for topic: {self.topic}")
        print(f"Sending interval: {min_interval}-{max_interval} seconds")
        
        try:
            while True:
                if self.sendData():
                    # Only sleep if send was successful
                    time_interval = random.randint(min_interval, max_interval)
                    print(f"\nWaiting {time_interval} seconds before next send...")
                    sleep(time_interval)

                else:
                    # If send failed, wait a short time before retrying
                    sleep(5)
                    
        except KeyboardInterrupt:
            print("\nStopping producer...")

        except Exception as e:
            print(f"Unexpected error: {e}")

        finally:
            print("Closing producer connection...")
            self.producer.flush()  # Ensure any buffered messages are sent
            self.producer.close()  # Ensure the producer is closed on exit

# Usage example
if __name__ == "__main__":
    producer = Producer()
    producer.run()