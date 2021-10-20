import grequests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import json

def get_href(html):
    try:
        return html.find('a', {'class': 'teaser-title'})['href']
    except:
        return None

def fetch(url, session):
    """
    asynchronous request
    """
    async with session.get(url) as response:
        response_json = await response.content
        return SimpleNamespace(**response_json)


def scratch(content):
    html = content.text.replace('\n', '').replace('\t', '')
    soup = BeautifulSoup(html, 'lxml')

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
            re.match(r'[a-zA-ZА-Яа-я]*([0-9.,]+)', price).group(1)
        if price_per_meter != '':
            price_per_meter = re.match(r'([0-9.,]+)', price_per_meter).group()
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


if __name__ == '__main__':
    start_time = time.time()
    N = 150
    urls = [f'https://realt.by/sale/offices/?page={i}' for i in range(N)]
    rs = (grequests.get(u) for u in urls)
    print(f'Finding objects...')
    finding_objects_time = time.time()
    responses = grequests.map(rs)
    responses = [x for x in responses if x is not None]
    s = []
    for x in responses:
        soup = BeautifulSoup(x.text, 'lxml')
        s.extend([get_href(x) for x in soup.find_all('div', {'class': 'listing-item'})])
    print(f'Found {len(s)} objects to parse. ({time.time() - finding_objects_time} s.)')
    print('Parsing...')
    rs = (grequests.get(u) for u in s)
    parsing_time = time.time()
    responses = grequests.map(rs)
    responses = [x for x in responses if x is not None]
    print(f"Parsed {len(responses)} objects. ({time.time() - parsing_time} s.)")

    # write to file
    pd.DataFrame([x.text for x in responses], columns = ['html']).to_excel('html.xlsx')

    data = []
    for x in responses:
        data.append(scratch(x))

    df = pd.DataFrame(data, columns=['lat', 'lon', 'district', 'streets', 'object_type', 'area',
                           'region', 'city', 'description', 'phone', 'price', 'prices_per_meter',
                           'agency'])
    df.to_excel('temp.xlsx')
    print(f"--- {time.time() - start_time} seconds ---")