"""
===============================================================================
Script: main.py
Description:
    Entry point for the IoT data collection system. This script imports and 
    executes the `run_service` function from the `iot_collector_service` module,
    which starts the core IOTService responsible for managing MQTT collectors 
    and handling SQL interactions.

Usage:
    Run this script directly to launch the IoT data service.

    Example:
        $ python main.py

Dependencies:
    - iot_collector_service.run_service

Author: [Martin P]

===============================================================================
"""

from iot_collector_service import run_service

if __name__ == '__main__':
    run_service()
