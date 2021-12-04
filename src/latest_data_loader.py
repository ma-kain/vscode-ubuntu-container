import sqlite3 as sl
import pandas as pd
import nse_india
import datetime
from one_time_data_loader import *

# Following resources and indices will be downloaded and imported into db
resources = [
    "historicalindices",
    "historical_pepb"
]

indices = [
    "NIFTY 50",
    "NIFTY NEXT 50"
]


def import_into_db(data, sql, con, index):
    rows = []
    for row in data[1:]:
        row = list(row)
        row[0] = datetime.datetime.strptime(row[0], '%d-%b-%Y').strftime('%Y-%m-%d')
        rows.append(row + [index])

    with con:
        print(con.executemany(sql, rows).rowcount, 'rows affected')


if __name__ == "__main__":
    print('Starting latest data loader...')
    
    db_con = sl.connect('/workspaces/vscode-ubuntu-container/resource/nse_india.db')
    
    start, end = None, datetime.date.today()

    with db_con:
        data = db_con.execute("SELECT max(Date) FROM HISTORICALINDICES WHERE Indices = 'NIFTY 50';").fetchall()[0]
        start = datetime.datetime.strptime(data[0], '%Y-%m-%d').date() + datetime.timedelta(days=1)

    print(f'Start: {start}, End: {end}.')

    if start >= end:
        print('Exit program as start is not greater than end.')
        print(exit)
        exit()

    for res in resources:
        sql = historicalindices_sql if res == 'historicalindices' else historical_pepb_sql
        for idx in indices:
            data = nse_india.NseIndia().get_data(res, idx, start, end)
            import_into_db(data, sql, db_con, idx)

