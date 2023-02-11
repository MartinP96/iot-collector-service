"""
    File name: __main__.py
    Date: 10.01.2023
"""

from .data_collector import  DataCollector
import json

def run_service():
    collector = DataCollector()
    collector.run_collector()

if __name__ == '__main__':
    run_service()
