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
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
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
                sensor_data = message.value
                temperatures.append(sensor_data['temperature'])
                humidities.append(sensor_data['humidity'])
                timestamps.append(time.time())
                print(f"Received data: {sensor_data}")  # Print received sensor data

        except KeyboardInterrupt:
            print("\nStopping consumer...")

        finally:
            self.consumer.close()

    def run(self):
        """
        Starts the consumer in a separate thread and displays the plot.
        """
        # Create and start the consumer thread
        consumer_thread = threading.Thread(target=self.consume_messages)
        consumer_thread.daemon = True  # Thread will exit when main program exits
        consumer_thread.start()
        
        # Show the plot (this will block until the window is closed)
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

# Usage
if __name__ == "__main__":
    consumer = Consumer()
    consumer.run()