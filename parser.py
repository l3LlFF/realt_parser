import time
import asyncio
import aiohttp
import re
import json
from bs4 import BeautifulSoup
import pandas as pd
from decouple import config

NUM_PAGES = config('NUM_PAGES', 2)
NUM_SEMAPHORES = config('NUM_SEMAPHORES', 5)
REALT_TYPE = config('REALT_TYPE', 'sale')
REALT_OBJECT = config('REALT_OBJECT', 'offices')

durations = []

def timed(func):
    """
    records approximate durations of function calls
    """
    def wrapper(*args, **kwargs):
        start = time.time()
        print(f'{func.__name__:<30} started')
        result = func(*args, **kwargs)
        duration = f'{func.__name__:<30} finished in {time.time() - start:.2f} seconds'
        print(duration)
        durations.append(duration)
        return result
    return wrapper

def get_proxy(filename):
    with open(filename, 'r') as f:
        data = f.read().splitlines()
        return [x.split(':') for x in data]


if __name__ == '__main__':
    urls = [f'https://realt.by/{REALT_TYPE}/{REALT_OBJECT}/?page={i}' for i in range(NUM_PAGES)]
    proxies = get_proxy('proxy.txt')

