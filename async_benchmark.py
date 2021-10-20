import time
import asyncio
import requests
import aiohttp

from types import SimpleNamespace

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


async def fetch(url, session):
    """
    asynchronous get request
    """
    async with session.get(url) as response:
        response_json = await response.json()
        return SimpleNamespace(**response_json)


async def fetch_many(loop, urls):
    """
    many asynchronous get requests, gathered
    """
    async with aiohttp.ClientSession() as session:
        tasks = [loop.create_task(fetch(url, session)) for url in urls]
        return await asyncio.gather(*tasks)

@timed
def sync_requests_get_all(urls):
    """
    performs synchronous get requests
    """
    # use session to reduce network overhead
    session = requests.Session()
    return [SimpleNamespace(**session.get(url).json()) for url in urls]


@timed
def async_requests_get_all(urls):
    """
    asynchronous wrapper around synchronous requests
    """
    loop = asyncio.get_event_loop()
    # use session to reduce network overhead
    session = requests.Session()

    async def async_get(url):
        return session.get(url)

    async_tasks = [loop.create_task(async_get(url)) for url in urls]
    return loop.run_until_complete(asyncio.gather(*async_tasks))


@timed
def asnyc_aiohttp_get_all(urls):
    """
    performs asynchronous get requests
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(fetch_many(loop, urls))


if __name__ == '__main__':
    # this endpoint takes ~3 seconds to respond,
    # so a purely synchronous implementation should take
    # little more than 30 seconds and a purely asynchronous
    # implementation should take little more than 3 seconds.
    urls = ['https://realt.by/newflats/object/715058/']*100

    #sync_requests_get_all(urls)
    #async_requests_get_all(urls)
    asnyc_aiohttp_get_all(urls)
    print('----------------------')
    [print(duration) for duration in durations]