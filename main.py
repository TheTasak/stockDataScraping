import json
import os

import requests as req
from bs4 import BeautifulSoup

stock = "XTB"

types = [
    "raporty-finansowe-rachunek-zyskow-i-strat",
    "raporty-finansowe-bilans",
    "raporty-finansowe-przeplywy-pieniezne",
    "wskazniki-wartosci-rynkowej",
    "wskazniki-rentownosci",
    "wskazniki-przeplywow-pienieznych",
    "wskazniki-zadluzenia",
    "wskazniki-plynnosci",
    "wskazniki-aktywnosci"
]


def get_data(data_type):
    url = f'https://www.biznesradar.pl/{data_type}/{stock}'

    webdata = req.get(url)
    if not webdata.ok:
        print("Could not get webdata")
        exit(400)

    soup = BeautifulSoup(webdata.text, 'lxml')
    table = soup.find("table", class_="report-table")
    try:
        os.mkdir(stock)
    except OSError as error:
        print(error)
    with open(f'{stock}/data_{stock}.txt', "w") as file:
        file.write(str(table))


def transform_data(data_type):
    all_data = []
    headers_data = ["Rok"]
    with open(f'{stock}/data_{stock}.txt', "r") as file:
        data = file.read()
    soup = BeautifulSoup(data, 'lxml')

    headers = soup.find("tr").find_all(class_="thq")
    for header in headers[:]:
        header_tr = header.text.replace("\n", "").replace("\t", "")
        header_tr = header_tr[:header_tr.find("(")]
        headers_data.append(header_tr)
    all_data.append(headers_data)

    for row in soup.find_all("tr")[1:]:
        row_data = []
        cells = row.find_all("td")
        header = cells[0]
        row_data.append(header.text)
        for cell in cells[1:-1]:
            value = cell.find(class_="value")
            row_data.append(value.text if value else "")
        all_data.append(row_data)

    json_data = []

    for index, date in enumerate(headers):
        obj = {}
        for value in all_data:
            obj[value[0]] = value[index + 1].replace(" ", "")
        json_data.append(obj)

    with open(f'{stock}/{stock}_{data_type}.txt', "w", encoding='utf-8') as file:
        file.write(json.dumps(json_data, indent=3, ensure_ascii=False))


for data_type in types:
    get_data(data_type)
    transform_data(data_type)
