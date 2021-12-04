import nse_india
from datetime import date
import pandas as pd
import sqlite3 as sl

if __name__ == "__main__":
    _con = sl.connect('nse_india.db')

    resource, index = 'historicalindices', 'NIFTY 50'
    _df = pd.concat([pd.read_csv(f'{resource}/{index}/{year}.csv') for year in range(1998, 2021)])
    _df['Indices'] = index
    _df.to_sql(resource.upper(), _con, if_exists='append')

    index = 'NIFTY NEXT 50'
    _df = pd.concat([pd.read_csv(f'{resource}/{index}/{year}.csv') for year in range(1999, 2021)])
    _df['Indices'] = index
    _df.to_sql(resource.upper(), _con, if_exists='append')



    resource, index = 'historical_pepb', 'NIFTY 50'
    _df = pd.concat([pd.read_csv(f'{resource}/{index}/{year}.csv') for year in range(1999, 2021)])
    _df['Indices'] = index
    _df.to_sql(resource.upper(), _con, if_exists='append')

    index = 'NIFTY NEXT 50'
    _df = pd.concat([pd.read_csv(f'{resource}/{index}/{year}.csv') for year in range(1999, 2021)])
    _df['Indices'] = index
    _df.to_sql(resource.upper(), _con, if_exists='append')


    # data = nse_india.get_nifty50_pepb_hist(date(2021, 12, 1), date.today())
    # print(data)
    # _df = nse_india.to_df(data)
    # print(_df.info())
    # print(_df)

    # print(get_nifty50_pepb_hist(date(2021, 12, 1), date.today()));
    # print(get_nifty_next50_price_hist(date(2021, 12, 1), date.today()));
    # print(get_nifty_next50_pepb_hist(date(2021, 12, 1), date.today()));

