import asyncio
from random import randint

async def download(code):
    wait_time = randint(1, 3)
    print('downloading {} will take {} second(s)'.format(code, wait_time))
    await asyncio.sleep(wait_time)  # I/O, context will switch to main function
    print('downloaded {}'.format(code))


async def fetch_many_semaphore(loop):
    sem = asyncio.Semaphore(3)

    async def safe_download(i):
        async with sem:  # semaphore limits num of simultaneous downloads
            return await download(i)
    tasks = [loop.create_task(safe_download(i)) for i in range(9)]
    await asyncio.gather(*tasks)  # await moment all downloads done


if __name__ ==  '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_many_semaphore(loop))
