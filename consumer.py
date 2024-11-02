# consumer.py
# -------------------------------------------------------------
# Filename: consumer.py
# Authors: Flores A., Ramirez A.
# Date: November 2024
# Description: 
#     This script defines a Kafka consumer class that reads 
#     sensor data (temperature and humidity) from a specified 
#     Kafka topic. The consumer continuously polls for new messages 
#     and updates live plots using Matplotlib to visualize the 
#     received sensor data.
# -------------------------------------------------------------

import json
import time
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from kafka import KafkaConsumer

# Global lists to store data for plotting
temperatures = []
humidities = []
timestamps = []

# Configuration
SERVER = 'lab9.alumchat.lol:9092'  # Address of the Kafka server
TOPIC = '21874'                     # Kafka topic to consume data from

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
        """
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[server],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='sensor_data_group'
        )
        self.topic = topic
        self.fig, (self.ax1, self.ax2) = plt.subplots(2, 1, figsize=(10, 6))
        self.ani = FuncAnimation(self.fig, self.update_plots, interval=1000, cache_frame_data=False)

    def run(self):
        """
        Continuously consumes sensor data from the configured Kafka topic.
        Updates the global lists for plotting.
        """
        print(f"Consuming messages from topic: {self.topic}")

        try:
            for message in self.consumer:
                sensor_data = message.value
                temperatures.append(sensor_data['temperature'])
                humidities.append(sensor_data['humidity'])
                timestamps.append(time.time())
                print(f"Received data: {sensor_data}")  # Print received sensor data

        except KeyboardInterrupt:
            print("Stopping consumer...")

        finally:
            self.consumer.close()  # Ensure the consumer is closed on exit

    def update_plots(self, frame):
        """
        Update the plots with the latest sensor data.
        
        Args:
            frame (int): The current frame number (not used in this context).
        """
        # Clear the axes for new data
        self.ax1.clear()
        self.ax2.clear()
        
        # Plot temperature data
        self.ax1.plot(timestamps, temperatures, label='Temperature', color='red')
        self.ax1.set_title('Temperature over Time')
        self.ax1.set_xlabel('Time (s)')
        self.ax1.set_ylabel('Temperature (°C)')
        self.ax1.legend()
        
        # Plot humidity data
        self.ax2.plot(timestamps, humidities, label='Humidity', color='blue')
        self.ax2.set_title('Humidity over Time')
        self.ax2.set_xlabel('Time (s)')
        self.ax2.set_ylabel('Humidity (%)')
        self.ax2.legend()

        # Adjust layout
        plt.tight_layout()
        plt.pause(0.1)  # Pause to allow for updates