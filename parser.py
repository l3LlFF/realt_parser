import time
import asyncio
import aiohttp
import re
import json
from bs4 import BeautifulSoup
import pandas as pd
from decouple import config
import aiofiles
import os
import random



NUM_PAGES = int(config('NUM_PAGES', 2))
NUM_SEMAPHORES = int(config('NUM_SEMAPHORES', 5))
REALT_TYPE = config('REALT_TYPE', 'sale')
REALT_OBJECT = config('REALT_OBJECT', 'offices')
SVDIR = config('IMAGES_FOLDER', '/')


durations = []
hrefs = []
result = []
image_refs = []


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
                district = td[1].find('a').text
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
        point = f"'SRID=4326;POINT ({lon} {lat})'::geometry"
    description = str(soup.find('div', {'class': 'top-description'}))
    try:
        agency = soup.find('div', {'class': 'agency-info-left'}).find('strong').text
    except:
        agency = ""
    images = [x['data-src'] for x in soup.find_all('a', {'class': 'object-gallery-item'})]
    return [point, district, street, object_type, x_area, region, city,
                 description, phone, price, price_per_meter, agency], images


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


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


async def fetch_hrefs(url, session, proxy):
    """
    asynchronous get request
    """
    host, port, login, password = proxy
    proxy_auth = aiohttp.BasicAuth(login, password)
    async with session.get(url, proxy=f"http://{host}:{port}",
                           proxy_auth=proxy_auth) as response:
        response_html = await response.text()
    soup = BeautifulSoup(response_html, 'lxml')
    def get_href(html):
        try:
            return html.find('a', {'class': 'teaser-title'})['href']
        except:
            return None

    hrefs.extend([get_href(x) for x in soup.find_all('div', {'class': 'listing-item'})])


async def fetch_data(url, session, proxy):
    """
    asynchronous get request
    """
    host, port, login, password = proxy
    proxy_auth = aiohttp.BasicAuth(login, password)
    async with session.get(url, proxy=f"http://{host}:{port}",
                           proxy_auth=proxy_auth) as response:
        response_html = await response.text()
    res, images = scratch(response_html)

    if res[0] == '':
        result.append(None)
    else:
        id = re.search(r'/([0-9]+)/', url).group(1)
        res = [id] + res
        image_refs.extend([(id, url) for url in images if 'realt' in url])
        result.append(res)


async def fetch_image(url, session, proxy):
    """
    asynchronous get request
    """
    host, port, login, password = proxy
    proxy_auth = aiohttp.BasicAuth(login, password)
    id, href = url
    if not os.path.exists(SVDIR):
        os.makedirs(SVDIR)
    folder = os.path.join(SVDIR, id)
    if not os.path.exists(folder):
        os.makedirs(folder)
        async with session.get(href, proxy=f"http://{host}:{port}",
                               proxy_auth=proxy_auth) as response:
            if response.status == 200:
                filename = f"{random.getrandbits(32)}.jpg"
                sv_path = os.path.join(SVDIR, id, filename)
                f = await aiofiles.open(sv_path, mode='wb')
                await f.write(await response.read())
                await f.close()


async def gather_tasks(loop, urls, function, proxies, n_semaphores):
    """
    gathers tasks
    """
    async def sem_task(task, semaphore):
        async with semaphore:
            await task

    for proxy, url_chunk in zip(proxies, urls):
        semaphore = asyncio.Semaphore(n_semaphores)
        async with aiohttp.ClientSession(trust_env=True, connector=aiohttp.TCPConnector(limit=64, ssl=False)) as session:
            tasks = [loop.create_task(sem_task(function(url, session, proxy), semaphore)) for url in url_chunk]
            return await asyncio.gather(*(task for task in tasks))


@timed
def async_run(urls, function, proxies, n_semaphores):
    """
    performs asynchronous function
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        gather_tasks(loop, urls, function, proxies, n_semaphores)
    )


def clear_list(lst):
    """
    clears list from None values
    """
    length = len(lst)
    cleared_list = [x for x in lst if x is not None]
    print(f'Links found: {len(cleared_list)}')
    print(f'Links lost: {length - len(cleared_list)}')
    return cleared_list


def to_database(data):

    urls = f"""postgresql://{config('DB_LOGIN')}:{config('DB_PASSWORD')}@{config('DB_HOST')}:{config('DB_PORT')}/{config('DB_NAME')}"""
    from sqlalchemy import create_engine
    engine = create_engine(urls)

    sql = f"""
        CREATE TABLE public.{config('REALT_TYPE')} (
        id int8 NULL,
        way geometry NULL,
        tags hstore NULL
        );
        
        CREATE INDEX {config('REALT_TYPE')}_y ON public.{config('REALT_TYPE')} USING gist (way);    
        
        ALTER TABLE {config('REALT_TYPE')}
        ADD CONSTRAINT id_{config('REALT_TYPE')} UNIQUE (id);
    """

    with engine.begin() as conn:
        try:
            cursor = conn.execute(sql)
        except Exception as e:
            print(e)
            pass
    df = data.drop_duplicates(['id'])

    df.loc[:, ('price')] = pd.to_numeric(df['price'].str.replace(',', '.'), errors='coerce')
    df.loc[:, ('prices_per_meter')] = pd.to_numeric(df['prices_per_meter'].str.replace(',', '.'), errors='coerce')
    df['area'] = df['area'].str.extractall(r"([0-9.,]+)[^0-9]*$").reset_index(level=1, drop=True)
    df = df.drop('description', axis=1)
    df.district = df.district.str.replace("\"", "`")
    df.agency = df.agency.str.replace("\"", "`")
    df['category'] = pd.qcut(df['price'], 3, labels=["low", "medium", "high"])
    df = df[~df['price'].isna()]
    columns = [x for x in df.columns if x not in ('id', 'way')]
    values = ','.join([f"""({i['id']}, {i['way']}, {repr(", ".join([f'{x[0]}=>"{x[1]}"' for x in zip(columns, i[columns]) if not pd.isna(x[1])]))}::hstore)"""
                       for i in list(df.to_records(index=False))])
    s = f"""
    INSERT INTO {config('REALT_TYPE')} (id, way, tags)
    values {values}
    ON CONFLICT (id) 
    DO 
       UPDATE SET 
       tags = {config('REALT_TYPE')}.tags || EXCLUDED.tags;"""
    with engine.begin() as conn:
        conn.execute(s.replace('%', '%%').replace("'null'", 'null'))


if __name__ == '__main__':
    urls = [f'https://realt.by/{REALT_TYPE}/{REALT_OBJECT}/?page={i}' for i in range(NUM_PAGES)]
    print(urls)
    proxies = get_proxy('proxy.txt')
    chunked_urls = chunks(urls, len(urls) // len(proxies))
    print(f"fetching links")
    async_run(urls=chunked_urls, function=fetch_hrefs,
              proxies=proxies, n_semaphores=NUM_SEMAPHORES)
    if len(hrefs) != 0:
        hrefs = clear_list(hrefs)
        chunked_urls = chunks(hrefs, len(hrefs) // len(proxies))
        print(f"fetching realt {config('REALT_TYPE', '')} objects")
        async_run(urls=chunked_urls, function=fetch_data,
                  proxies=proxies, n_semaphores=NUM_SEMAPHORES)
        result = clear_list(result)
        chunked_urls = chunks(image_refs, len(image_refs) // len(proxies))
        df = pd.DataFrame(result, columns=['id', 'way', 'district', 'street', 'object_type', 'area',
                                         'region', 'city', 'description', 'phone', 'price', 'prices_per_meter',
                                         'agency'])
        df.to_excel('data.xlsx')
        to_database(df)
        print(f"fetching realt {config('REALT_TYPE', '')} images")
        async_run(urls=chunked_urls, function=fetch_image,
                  proxies=proxies, n_semaphores=NUM_SEMAPHORES)

