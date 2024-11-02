# main.py
# -------------------------------------------------------------
# Filename: main.py
# Authors: Flores A., Ramirez A.
# Date: November 2024
# Description: 
#     This script initializes a Kafka producer and consumer to read 
#     sensor data from a specified Kafka topic and visualize it live 
#     using Matplotlib. The producer generates mock sensor data, while 
#     the consumer receives and processes this data to update the plots.
# -------------------------------------------------------------

from producer import Producer
from consumer import Consumer
import threading
import sys

# Create an instance of the Producer
producer = Producer()

# Create an instance of the Consumer
consumer = Consumer()

# Start the producer in a separate thread
producer_thread = threading.Thread(target=producer.run)
producer_thread.daemon = True
producer_thread.start()

# Start the consumer in a separate thread
consumer_thread = threading.Thread(target=consumer.run)
consumer_thread.daemon = True
consumer_thread.start()

try:
    # Run indefinitely until interrupted
    while True:
        pass  # Keep the main thread alive

except KeyboardInterrupt:
    print("Keyboard interrupt received, stopping...")

finally:
    # Properly close the consumer and producer threads if necessary
    consumer_thread.join(timeout=1)
    producer_thread.join(timeout=1)
    print("Threads have been closed. Exiting the program.")
    sys.exit(0)