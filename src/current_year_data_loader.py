import sqlite3 as sl
import pandas as pd
import nse_india
import datetime
import one_time_data_loader


current_year = 2021


def import_into_db(data, sql, con, index):
    rows = []
    for row in data[1:]:
        row = list(row)
        row[0] = datetime.datetime.strptime(row[0], '%d-%b-%Y').strftime('%Y-%m-%d')
        rows.append(row + [index])

    with con:
        print(con.executemany(sql, rows).rowcount, 'rows affected')


if __name__ == "__main__":
    db_con = sl.connect('nse_india.db')
    
    start, end = None, datetime.date.today()

    with db_con:
        data = db_con.execute("SELECT max(Date) FROM HISTORICALINDICES WHERE Indices = 'NIFTY 50';").fetchall()[0]
        start = datetime.datetime.strptime(data[0], '%Y-%m-%d').date() + datetime.timedelta(days=1)

    print(f'Start: {start}, End: {end}.')

    if start > end:
        print('Exit program as start is greater than end.')
        print(exit)
        exit()
    
    resource, index = 'historicalindices', 'NIFTY 50'
    data = nse_india.NseIndia().get_data(resource, index, start, end)
    import_into_db(data, one_time_data_loader.historicalindices_sql, db_con, index)

    index = 'NIFTY NEXT 50'
    data = nse_india.NseIndia().get_data(resource, index, start, end)
    import_into_db(data, one_time_data_loader.historicalindices_sql, db_con, index)


    resource, index = 'historical_pepb', 'NIFTY 50'
    data = nse_india.NseIndia().get_data(resource, index, start, end)
    import_into_db(data, one_time_data_loader.historical_pepb_sql, db_con, index)

    index = 'NIFTY NEXT 50'
    data = nse_india.NseIndia().get_data(resource, index, start, end)
    import_into_db(data, one_time_data_loader.historical_pepb_sql, db_con, index)

