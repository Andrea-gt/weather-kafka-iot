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
import threading
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from kafka import KafkaConsumer
from data_generator import decode
import signal
import sys

# Global lists to store data for plotting
temperatures = []
humidities = []
timestamps = []

# Configuration
SERVER = 'lab9.alumchat.lol:9092'  # Address of the Kafka server
TOPIC = '21874'  # Kafka topic to consume data from

class Consumer:
    def __init__(self, server=SERVER, topic=TOPIC):
        """
        Initializes the Kafka consumer with the given server and topic.
        """
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[server],
            value_deserializer=lambda x: x,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='sensor_data_group'
        )
        self.topic = topic
        self.fig, (self.ax1, self.ax2) = plt.subplots(2, 1, figsize=(10, 6))
        self.ani = FuncAnimation(self.fig, self.update_plots, interval=1000, cache_frame_data=False)

    def consume_messages(self):
        """
        Continuously consumes sensor data from the configured Kafka topic.
        This method runs in a separate thread.
        """
        print(f"\nConsuming messages from topic: {self.topic}")

        try:
            for message in self.consumer:
                sensor_data = decode(message.value)
                temperatures.append(sensor_data['temperature'])
                humidities.append(sensor_data['humidity'])
                timestamps.append(time.time())
                print(f"\nReceived data: {message.value}")  # Print received sensor data
                print(f"Decoded data: {sensor_data}")  # Print decoded sensor data

        except Exception as e:
            print(f"\nError in consumer thread: {e}")

        finally:
            self.signal_handler()

    def run(self):
        """
        Starts the consumer in a separate thread and displays the plot.
        """
        # Handle KeyboardInterrupt for a clean exit
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # Create and start the consumer thread
        consumer_thread = threading.Thread(target=self.consume_messages)
        consumer_thread.daemon = True  # Thread will exit when main program exits
        consumer_thread.start()
        
        # Start the Matplotlib event loop
        plt.show()

    def update_plots(self, frame):
        """
        Update the plots with the latest sensor data.
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

    def signal_handler(self, sig = None, frame = None):
        """
        Handles the interrupt signal and closes the plot window.
        """
        print("\nInterrupt received. Closing consumer and plot...")
        self.consumer.close()
        plt.close(self.fig)
        sys.exit(0)

# Usage
if __name__ == "__main__":
    consumer = Consumer()
    consumer.run()