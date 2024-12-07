import json
import os
import sys
import argparse
import time

import pandas as pd
import requests as req
from bs4 import BeautifulSoup
from pathlib import Path

data_folder = "data"
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
    handle = Path(path)
    handle.mkdir(parents=True, exist_ok=True)


def get_price_data(stock, delay, iters):
    stock_dir = f'{data_folder}/{stock}'
    handle_path(stock_dir)

    for i in range(0, iters):
        url = f'https://www.biznesradar.pl/notowania-historyczne/{stock},{i+1}'
        webdata = req.get(url)
        if not webdata.ok:
            print("Could not get webdata")
            return

        soup = BeautifulSoup(webdata.text, 'lxml')
        table = soup.find("table", class_="qTableFull")
        if table is None:
            return
        with open(f'{stock_dir}/data_price_{stock}_{i}.txt', "w") as file:
            file.write(str(table))
        time.sleep(delay/1000)
    

def transform_price_data(stock, iters, output):
    stock_dir = f'{data_folder}/{stock}'
    if not os.path.exists(stock_dir):
        raise Exception("Directory with stock data doesn't exist")
        return

    all_data = []
    header_data = []
    for i in range(0, iters):
        try:
            file = open(f'{stock_dir}/data_price_{stock}_{i}.txt', "r")  
        except:
            break
        data = file.read()
        file.close()
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

    if output == "json":
        json_data = []

        for value in all_data:
            obj = {}
            for index, cell in enumerate(value):
                obj[header_data[index]] = cell
            json_data.append(obj)

        with open(f'{stock_dir}/{stock}_price.txt', "w", encoding='utf-8') as file:
            file.write(json.dumps(json_data, indent=3, ensure_ascii=False))
    elif output == "csv":
        data_dict = {}
        for index, value in enumerate(all_data):
            data_dict[index] = value
        df = pd.DataFrame.from_dict(data_dict, orient="index", columns=header_data)
        df.to_csv(f'{stock_dir}/{stock}_price.csv', index=False)


def get_financial_data(data_type, stock, delay):
    stock_dir = f'{data_folder}/{stock}'
    handle_path(stock_dir)
    url = f'https://www.biznesradar.pl/{data_type}/{stock}'

    webdata = req.get(url)
    if not webdata.ok:
        print("Could not get webdata")
        exit(400)

    soup = BeautifulSoup(webdata.text, 'lxml')
    table = soup.find("table", class_="report-table")
    with open(f'{stock_dir}/data_{stock}.txt', "w") as file:
        file.write(str(table))
    time.sleep(delay/1000)


def transform_financial_data(data_type, stock, output):
    stock_dir = f'{data_folder}/{stock}'
    all_data = []
    headers_data = ["Rok"]
    if not os.path.exists(stock_dir):
        raise Exception("Directory with stock data doesn't exist")
        return
    with open(f'{stock_dir}/data_{stock}.txt', "r") as file:
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
            row_data.append(value.text.replace(" ", "") if value else "")
        all_data.append(row_data)

    if output == "json":
        json_data = []

        for index, date in enumerate(headers):
            obj = {}
            for value in all_data:
                obj[value[0]] = value[index + 1].replace(" ", "")
            json_data.append(obj)

        with open(f'{stock_dir}/{stock}_{data_type}.txt', "w", encoding='utf-8') as file:
            file.write(json.dumps(json_data, indent=3, ensure_ascii=False))
    elif output == "csv":
        data_dict = {}
        for index, value in enumerate(all_data[1:]):
            data_dict[index] = value
        df = pd.DataFrame.from_dict(data_dict, orient="index", columns=all_data[0])
        df.to_csv(f'{stock_dir}/{stock}_{data_type}.csv', index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("BiznesRadar Scrapper")
    parser.add_argument("--type", help="Specify type of data which you want to scrap. Available options: price, financial", type=str)
    parser.add_argument("--stock", help="Specify stocks to scrap, delimited by a comma", type=str)
    parser.add_argument("--max_iters", nargs="?", help="Maximum number of iterations when scrapping price", type=int, default=5)
    parser.add_argument("--delay", nargs="?", help="Delay after making each web request (in milliseconds)", type=int, default=100)
    parser.add_argument("--output", nargs="?", help="Output type. Available options: json, csv", type=str, default="json")
    args = parser.parse_args()
    if args.type == "financial" and args.stock is not None:
        for stock in args.stock.split(","):
            stock = stock.strip()
            for data_type in types:
              get_financial_data(data_type, stock, args.delay)
              transform_financial_data(data_type, stock, args.output)
    elif args.type == "price" and args.stock is not None:
        for stock in args.stock.split(","):
            stock = stock.strip()
            get_price_data(stock, args.delay, args.max_iters)
            transform_price_data(stock, args.max_iters, args.output)
    elif args.type is None or len(args.type) == 0:
        print("No type specified. Type -h to view all possible arguments")
    else:
        print("Wrong type or stock specified")
