import asyncio
import random
import aiohttp
import time
from parser import scratch
responses = []
durations = []



def timed(func):
    """
    records approximate durations of function calls
    """
    def wrapper(*args, **kwargs):
        start = time.time()
        print(f'{func.__name__:<30} started')
        result = func(*args, **kwargs)
        dur = time.time() - start
        duration = f'{func.__name__:<30} finished in {dur:.2f} seconds'
        print(duration)
        durations.append(round(dur, 3))
        return result
    return wrapper

async def download(code):
    wait_time = random.randint(1, 3)
    print('downloading {} will take {} second(s)'.format(code, wait_time))
    await asyncio.sleep(wait_time)  # I/O, context will switch to main function
    print('downloaded {}'.format(code))
    return code

async def fetch(url):
    async with aiohttp.ClientSession(trust_env=True, connector=aiohttp.TCPConnector(limit=64, ssl=False)) as session:
        host, port, login, password = ['91.215.87.243', '8000', 'NPtbNu', 'WQy0am']
        proxy_auth = aiohttp.BasicAuth(login, password)
        async with session.get(url, proxy=f"http://{host}:{port}",
                               proxy_auth=proxy_auth) as response:
            response_html = await response.text()
            result = scratch(response_html)
            if result[0] == '':
                responses.append(None)
                return None
            else:
                responses.append(result)
                return 'OK'


async def main(loop, no_concurrent = 50, n_tasks = 100):

    dltasks = set()
    i = 0
    while i < n_tasks:
        if len(dltasks) >= no_concurrent:
            # Wait for some download to finish before adding a new one
            _done, dltasks = await asyncio.wait(
                dltasks, return_when=asyncio.FIRST_COMPLETED)

        dltasks.add(loop.create_task(fetch('https://realt.by/sale/offices/object/1477813/')))
        i += 1
    # Wait for the remaining downloads to finish
    await asyncio.wait(dltasks)

@timed
def async_run(no_concurrent, n_tasks):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
            main(loop, no_concurrent=no_concurrent, n_tasks=n_tasks)
        )

concurrent = list(range(20, 30, 1))
responses = list(range(10, 100, 10))
n_res = 223
for n_con in [24]:
    responses = []
    print(f'n_col = {n_con}')
    print(f'n_res = {n_res}')
    async_run(n_con, n_res)
    length = len(responses)
    cleared_list = [x for x in responses if x is not None]
    print(f'Links found: {len(cleared_list)}')
    print(f'Links lost: {length - len(cleared_list)}')
    print('-' * 20)
print(durations)
print(concurrent)
