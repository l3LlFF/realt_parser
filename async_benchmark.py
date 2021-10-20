import time
import asyncio
import requests
import aiohttp
import re
import json
from bs4 import BeautifulSoup
from types import SimpleNamespace
import pandas as pd

durations = []
result = []
hrefs = []


def get_urls(content):
    soup = BeautifulSoup(content, 'lxml')

    def get_href(html):
        try:
            return html.find('a', {'class': 'teaser-title'})['href']
        except:
            return None

    hrefs.extend([get_href(x) for x in soup.find_all('div', {'class': 'listing-item'})])


def scratch(content):
    html = content.replace('\n', '').replace('\t', '')
    soup = BeautifulSoup(html, 'lxml')
    district = street = object_type = x_area = region = city = ''
    for el in soup.find_all('table'):
        tr = el.find_all('tr')

        for e in tr:
            td = e.find_all('td')
            if len(td) < 2:
                continue
            key = td[0].text
            value = td[1].text
            if key == 'Район города':
                district = value
            elif key == 'Адрес':
                street = value
            elif key == 'Вид объекта':
                object_type = value
            elif key == 'Площадь':
                x_area = value
            elif key == 'Область':
                region = value
            elif key == 'Населенный пункт':
                city = value
    try:
        phone = soup.find('div', {'class': 'object-contacts'}).find('strong').text
    except:
        phone = ''
    price_block = soup.find('a', {'data-currency': '840', 'rel': 'tooltip'})
    if price_block is None:
        price = ''
        price_per_meter = ''
    else:
        price = price_block['data-price'].replace(' ', '')
        price_per_meter = price_block['data-price_m2'].replace(' ', '')
        if price != '':
            price = re.match(r'[a-zA-ZА-Яа-я]*([0-9.,]+)', price).group(1)
        if price_per_meter != '':
            price_per_meter = re.match(r'[a-zA-ZА-Яа-я]*([0-9.,]+)', price_per_meter).group(1)
    location = soup.find('div', {'id': 'map-center'})
    if location is None:
        lon = ''
        lat = ''
    else:
        position_block = json.loads(location['data-center'])['position.']
        lon = position_block['x']
        lat = position_block['y']
    description = str(soup.find('div', {'class': 'top-description'}))
    try:
        agency = soup.find('div', {'class': 'agency-info-left'}).find('strong').text
    except:
        agency = ""
    return [lat, lon, district, street, object_type, x_area, region, city,
                 description, phone, price, price_per_meter, agency]


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


async def fetch_urls(url, session):
    """
    asynchronous get request
    """
    async with session.get(url) as response:
        response_html = await response.text()
    get_urls(response_html)


async def fetch_data_proxy(url, session, proxy):
    """
    asynchronous get request
    """
   # print(f'Getting {url}')
    proxy_auth = aiohttp.BasicAuth(proxy['login'], proxy['password'])
    async with session.get(url,proxy=proxy['host'],
                           proxy_auth=proxy_auth) as response:
        response_html = await response.text()
    result.append(scratch(response_html))
    #print(f'Parsed {url}')


async def fetch_data(url, session):
    """
    asynchronous get request
    """
    #print(f'Getting {url}')
    async with session.get(url) as response:
        response_html = await response.text()
    result.append(scratch(response_html))
    #print(f'Parsed {url}')


async def fetch_many(loop, urls, function):
    """
    many asynchronous get requests, gathered
    """
    async with aiohttp.ClientSession() as session:
        tasks = [loop.create_task(function(url, session)) for url in urls]
        return await asyncio.gather(*tasks)

async def fetch_many_semaphore(loop, urls, function, n):
    semaphore = asyncio.Semaphore(n)
    semaphore1 = asyncio.Semaphore(n)

    async def sem_task(task, semaphore):
        async with semaphore:
            await task
    async with aiohttp.ClientSession() as session:
        proxy = {
            'login': 'NPtbNu',
            'password': 'WQy0am',
            'host': 'http://91.215.87.243:8000'
        }
        tasks = [loop.create_task(sem_task(function(url, session), semaphore)) for url in urls[:len(urls)//2]]
        return await asyncio.gather(*(task for task in tasks))


@timed
def asnyc_aiohttp_get_all_semaphore(urls, function, n_semaphores):
    """
    performs asynchronous get requests
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(fetch_many_semaphore(loop, urls, function, n_semaphores))

@timed
def asnyc_aiohttp_get_all(urls, function):
    """
    performs asynchronous get requests
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(fetch_many(loop, urls, function))


if __name__ == '__main__':
    N = 150
    N_SEMAPHORES = 21 # 21
    urls = [f'https://realt.by/sale/offices/?page={i}' for i in range(N)]
    print(f'Finding objects...')
    asnyc_aiohttp_get_all_semaphore(urls, fetch_urls, N_SEMAPHORES)
    print(len(hrefs))
    hrefs = [x for x in hrefs if x is not None]

    pd.DataFrame(hrefs, columns=['href']).to_excel('hrefs.xlsx')
    print(f'Found {len(hrefs)} objects to parse.')
    print('Parsing...')
    asnyc_aiohttp_get_all_semaphore(hrefs, fetch_data, N_SEMAPHORES)
    print(f"Parsed {len(result)} objects.")
    print(f"Empty = {len([x for x in result if x[0] == ''])}")
    print(f"Full = {len([x for x in result if x[0] != ''])}")
    pd.DataFrame(result, columns=['lat', 'lon', 'district', 'streets', 'object_type', 'area',
                                     'region', 'city', 'description', 'phone', 'price', 'prices_per_meter',
                                     'agency']).to_excel('result.xlsx')

    print('----------------------')
    [print(duration) for duration in durations]