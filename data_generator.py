# -------------------------------------------------------------
# Filename: data_generator.py
# Author: Flores A., Ramirez A.
# Date: Nov 2024
#
# Description: 
#     This script generates simulated sensor data readings, including 
#     temperature, humidity, and wind direction, to mimic the outputs 
#     of a typical weather station. The temperature and humidity values 
#     are randomized around specific averages, constrained within realistic 
#     ranges, while the wind direction is chosen randomly from the main 
#     compass points. The data is returned as a JSON-compatible dictionary 
#     for easy integration and further processing in applications.
# -------------------------------------------------------------

import random
import struct

# Assign 3-bit binary values for each wind direction
wind_directions = {
    "N": 0b000,
    "NE": 0b001,
    "E": 0b010,
    "SE": 0b011,
    "S": 0b100,
    "SW": 0b101,
    "W": 0b110,
    "NW": 0b111
}

def getSensorData():
    """
    Generates a dictionary containing random sensor readings for temperature, 
    humidity, and wind direction. Each reading falls within specified ranges:
    - Temperature (in degrees): 0 - 110
    - Humidity (in percentage): 0 - 100
    - Wind direction (compass direction): one of the eight main compass directions

    Returns:
        dict: Sensor data with keys 'temperature', 'humidity', and 'wind_direction'.
    """
    
    # Generate a random temperature, centered at 55°F with a standard deviation of 10
    # and restrict it within the range of 0 to 110°F.
    temperature = max(0, min(round(random.normalvariate(55, 10), 2), 110))
    
    # Generate a random humidity percentage, centered at 55% with a standard deviation of 15
    # and restrict it within the range of 0 to 100%.
    humidity = max(0, min(int(random.normalvariate(55, 15)), 100))
    
    # Select a random wind direction from the eight main compass directions.
    wind_direction = random.choice(["N", "NW", "W", "SW", "S", "SE", "E", "NE"])

    # Store the generated data in a dictionary.
    sensor_data = {
        "temperature": temperature,
        "humidity": humidity,
        "wind_direction": wind_direction
    }

    # Return the sensor data dictionary.
    return sensor_data

def encode(data):
    """
    Encodes the sensor data into a bytes payload suitable for Kafka.
    Args:
        data (dict): Sensor data dictionary with 'temperature', 'humidity', and 'wind_direction'.
    Returns:
        bytes: Encoded payload ready for Kafka
    """

    # Retrieve and encode each sensor reading
    temperature = int(data["temperature"] * 100) & 0b111111111111  # 12 bits for temperature (0-409.99)
    humidity = int(data["humidity"]) & 0b1111111                   # 7 bits for humidity
    wind_direction = wind_directions[data["wind_direction"]]       # 3 bits for wind direction

    # Combine all into a 32-bit payload (4 bytes)
    payload = (temperature << 20) | (humidity << 13) | (wind_direction << 10)

    # Convert to bytes - big endian ('>I')
    return struct.pack('>I', payload)

def decode(data):
    """
    Decodes the bytes payload from Kafka back into sensor data.
    Args:
        data (bytes): Encoded payload from Kafka
    Returns:
        dict: Decoded sensor data
    """

    # Unpack bytes back to integer
    payload = struct.unpack('>I', data)[0]
    
    # Extract each value
    temperature = ((payload >> 20) & 0b111111111111) / 100  # Convert back to float
    humidity = (payload >> 13) & 0b1111111
    wind_direction_bits = (payload >> 10) & 0b111
    
    # Map back to wind direction
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    wind_direction = directions[wind_direction_bits]
    
    return {
        "temperature": round(temperature, 2),
        "humidity": humidity,
        "wind_direction": wind_direction
    }