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