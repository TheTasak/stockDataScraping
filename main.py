import json
import os
import sys
import argparse

import requests as req
from bs4 import BeautifulSoup

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


def handle_path(path):
    if not os.path.exists(path):
        try:
            os.mkdir(path)
        except OSError as error:
            print(error)
            return False
    return True


def get_price_data(stock, iters):
    if not handle_path(stock):
        return

    for i in range(0, iters):
        url = f'https://www.biznesradar.pl/notowania-historyczne/{stock},{i+1}'
        webdata = req.get(url)
        if not webdata.ok:
            print("Could not get webdata")
            exit(400)

        soup = BeautifulSoup(webdata.text, 'lxml')
        table = soup.find("table", class_="qTableFull")
        with open(f'{stock}/data_price_{stock}_{i}.txt', "w") as file:
            file.write(str(table))
    

def transform_price_data(stock, iters):
    if not os.path.exists(stock):
        raise Exception("Directory with stock data doesn't exist")
        return

    all_data = []
    header_data = []
    for i in range(0, iters):
        with open(f'{stock}/data_price_{stock}_{i}.txt', "r") as file:
            data = file.read()
            soup = BeautifulSoup(data, 'lxml')
            if i == 0:
                headers = soup.find("tr").find_all("th")
                for header in headers[:]:
                   header_data.append(header.text) 
            rows = soup.find_all("tr")[1:]
            for row in rows:
                row_data = []
                for cell in row.find_all("td"):
                    row_data.append(cell.text.replace(" ", ""))
                all_data.append(row_data)

    json_data = []

    for value in all_data:
        obj = {}
        for index, cell in enumerate(value):
            obj[header_data[index]] = cell
        json_data.append(obj)

    with open(f'{stock}/{stock}_price.txt', "w", encoding='utf-8') as file:
        file.write(json.dumps(json_data, indent=3, ensure_ascii=False))


def get_financial_data(data_type, stock):
    url = f'https://www.biznesradar.pl/{data_type}/{stock}'

    webdata = req.get(url)
    if not webdata.ok:
        print("Could not get webdata")
        exit(400)

    soup = BeautifulSoup(webdata.text, 'lxml')
    table = soup.find("table", class_="report-table")
    if not os.path.exists(stock):
        try:
            os.mkdir(stock)
        except OSError as error:
            print(error)
    with open(f'{stock}/data_{stock}.txt', "w") as file:
        file.write(str(table))


def transform_financial_data(data_type, stock):
    all_data = []
    headers_data = ["Rok"]
    if not os.path.exists(stock):
        raise Exception("Directory with stock data doesn't exist")
        return
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser("BiznesRadar Scrapper")
    parser.add_argument("--type", help="Specify type of data which you want to scrap. Available options: price, financial", type=str)
    parser.add_argument("--stock", help="Specify stock to scrap", type=str)
    parser.add_argument("--iters", nargs="?", help="Number of iterations when scrapping price", type=int, default=5)
    args = parser.parse_args()
    if args.type == "financial":
        for data_type in types:
          get_financial_data(data_type, args.stock)
          transform_financial_data(data_type, args.stock)
    elif args.type == "price":
        get_price_data(args.stock, args.iters)
        transform_price_data(args.stock, args.iters)
    else:
        print("Wrong type specified")
