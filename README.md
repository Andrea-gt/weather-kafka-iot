# Kafka Sensor Data Producer and Consumer

This project consists of a Kafka producer that generates and sends sensor data (temperature, humidity, wind direction) to a Kafka topic, and a consumer that retrieves this data, decodes it, and visualizes it using Matplotlib.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Code Overview](#code-overview)
  - [Producer](#producer)
  - [Consumer](#consumer)

## Features

- Generate random sensor data with configurable parameters.
- Send sensor data to a specified Kafka topic.
- Consume data from the Kafka topic and visualize it in real time.
- Handle data serialization and deserialization.
- Graceful shutdown of both producer and consumer on interrupt.

## Requirements

- Python 3.6 or higher
- [Kafka](https://kafka.apache.org/)
- [Kafka Python](https://kafka-python.readthedocs.io/en/master/)
- [Matplotlib](https://matplotlib.org/)
- [NumPy](https://numpy.org/)
- [pandas](https://pandas.pydata.org/)

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/Andrea-gt/weather-kafka-iot.git
   cd weather-kafka-iot
   ```

2. Install the required packages:

   ```bash
   pip install kafka-python matplotlib numpy pandas
   ```

3. Ensure that your Kafka server is running and create a topic if necessary.

## Usage

### Running the Producer

To start the producer, run:

```bash
python producer.py
```

You can specify the minimum and maximum intervals for sending data in the `Producer` class.

### Running the Consumer

To start the consumer, run:

```bash
python consumer.py
```

This will continuously consume messages from the specified Kafka topic and visualize the sensor data in real time.

## Code Overview

### Producer

The **Producer** class is responsible for generating and sending sensor data to a specified Kafka topic. It connects to the Kafka server and uses the `KafkaProducer` to send encoded sensor data. The main functionalities include:

- **Initialization**: Establishes a connection to the Kafka server and configures the producer with necessary parameters such as `acks` and `request_timeout_ms`.
- **Data Generation and Sending**: Generates random sensor data using the `getSensorData()` function, encodes it with the `encode()` function, and sends it to the Kafka topic. The `sendData()` method handles this process, including error management.
- **Continuous Sending**: The `run()` method allows the producer to send data at random intervals, ensuring a steady flow of sensor data to the Kafka topic. It handles user interrupts to stop sending data gracefully.

### Consumer

The **Consumer** class is designed to consume sensor data from the Kafka topic and visualize it using Matplotlib. It continuously listens for messages and updates the visualizations in real time. Key features include:

- **Initialization**: Connects to the Kafka server and initializes the `KafkaConsumer` to listen to a specific topic, starting from the earliest available message.
- **Message Consumption**: The `consume_messages()` method continuously polls for new messages, decodes the received data using the `decode()` function, and updates lists that store temperature, humidity, and timestamps for visualization.
- **Real-Time Visualization**: The `update_plots()` method updates Matplotlib plots with the latest sensor data, displaying temperature and humidity over time.
- **Graceful Shutdown**: The `signal_handler()` method handles keyboard interrupts, ensuring that the consumer closes its connection and exits cleanly.
