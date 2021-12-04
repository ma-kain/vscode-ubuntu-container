import sqlite3 as sl
import pandas as pd
import csv
import datetime


historicalindices_sql = '''INSERT INTO HISTORICALINDICES (Date, Open, High, Low, Close, "Shares Traded", "Turnover (Rs. Cr)", Indices) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)'''

historical_pepb_sql = '''INSERT INTO HISTORICAL_PEPB (DATE, "P/E", "P/B", "Div Yield", Indices) 
            VALUES (?, ?, ?, ?, ?)'''


def create_tables(con):
    with con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS HISTORICALINDICES (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                "Date" TEXT,
                "Open" REAL,
                "High" REAL,
                "Low" REAL,
                "Close" REAL,
                "Shares Traded" INTEGER,
                "Turnover (Rs. Cr)" REAL,
                "Indices" TEXT
            );
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS HISTORICAL_PEPB (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                "Date" TEXT,
                "P/E" REAL,
                "P/B" REAL,
                "Div Yield" REAL,
                "Indices" TEXT
            );
        """)



def import_into_db(resource, index, year, sql, con):
    rows = []
    with open(f'{resource}/{index}/{year}.csv', 'r') as file:
        csvreader = csv.reader(file)
        _ = next(csvreader)
        for row in csvreader:
            row[0] = datetime.datetime.strptime(row[0], '%d-%b-%Y').strftime('%Y-%m-%d')
            rows.append(row + [index])

    with con:
        print(con.executemany(sql, rows).rowcount, 'rows affected')


if __name__ == "__main__":
    print('No longer required as data already loaded.')
    print(exit)
    exit()

    db_con = sl.connect('nse_india.db')

    create_tables(db_con)

    resource, index = 'historicalindices', 'NIFTY 50'
    for year in range(1998, 2021):
        import_into_db(resource, index, year, historicalindices_sql, db_con)

    index = 'NIFTY NEXT 50'
    for year in range(1999, 2021):
        import_into_db(resource, index, year, historicalindices_sql, db_con)


    resource, index = 'historical_pepb', 'NIFTY 50'
    for year in range(1999, 2021):
        import_into_db(resource, index, year, historical_pepb_sql, db_con)

    index = 'NIFTY NEXT 50'
    for year in range(1999, 2021):
        import_into_db(resource, index, year, historical_pepb_sql, db_con)

