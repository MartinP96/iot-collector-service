"""
    File name: __main__.py
    Date: 10.01.2023
"""
from .iot_service import IOTService

def run_service():
    service = IOTService()
    service.service_run()
