import sqlite3 as sl
from sqlite3.dbapi2 import Error
import pandas as pd
import nse_india
import datetime
from csv_one_time_data_loader import *


db_con = sl.connect('/workspaces/vscode-ubuntu-container/resource/nse_india.db')

# Following resources and indices will be downloaded and imported into db
resources = [
    "historicalindices",
    "historical_pepb"
]


indices = [
    "NIFTY AUTO",
    "NIFTY BANK",
    "NIFTY REALTY",
    "NIFTY INFRASTRUCTURE",
    "NIFTY COMMODITIES",
    "NIFTY FMCG",
    "NIFTY PHARMA"
]


def import_into_db(data, sql, con, index):
    rows = []
    for row in data[1:]:
        row = list(row)
        row[0] = datetime.datetime.strptime(row[0], '%d-%b-%Y').strftime('%Y-%m-%d')
        rows.append(row + [index])

    with con:
        print(con.executemany(sql, rows).rowcount, 'rows affected')


def get_start_date(res, idx):
    start = datetime.date(1999, 1, 1)
    table = 'HISTORICALINDICES' if res == 'historicalindices' else 'HISTORICAL_PEPB'
    data = None
    with db_con:
        data = db_con.execute(f"SELECT max(Date) FROM {table} WHERE Indices = '{idx}';").fetchall()
    
    if data[0][0]:
        start = datetime.datetime.strptime(data[0][0], '%Y-%m-%d').date() + datetime.timedelta(days=1)

    return start


if __name__ == "__main__":
    print('Starting data loader...')

    today = datetime.date.today()

    for res in resources:
        sql = historicalindices_sql if res == 'historicalindices' else historical_pepb_sql

        for idx in indices:
            start = get_start_date(res, idx)
            
            while start < today:
                end = start + datetime.timedelta(days=364)
                print(f'Download Resource: {res}, Index: {idx}, Start: {start}, End: {end}.')
                try:
                    data = nse_india.NseIndia().get_data(res, idx, start, end)
                    import_into_db(data, sql, db_con, idx) if data else None
                    print('Success')
                except Exception as e:
                    print('Failed')
                    print(e)

                start = end + datetime.timedelta(days=1)

