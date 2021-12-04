import urllib.request
import gzip
from bs4 import BeautifulSoup
from datetime import date
import pandas as pd


class NseIndex:
    NIFTY_50 = 'NIFTY 50'
    NIFTY_NEXT_50 = 'NIFTY NEXT 50'


class NseResource:
    PEPB = 'historical_pepb'
    PRICES = 'historicalindices'


class UrlBuilder:

    def __init__(self):
        self._base_url = 'https://www1.nseindia.com/products/dynaContent/equities/indices'


    @staticmethod
    def _strfdate(dt: date):
        return dt.strftime('%d-%m-%Y')


    def _pepb_values_url(self, index, start, end):
        uri = f'historical_pepb.jsp?indexName={urllib.parse.quote(index)}'
        uri += f'&fromDate={self._strfdate(start)}&toDate={self._strfdate(end)}'
        uri += '&yield1=undefined&yield2=undefined&yield3=undefined&yield4=all'
        return f'{self._base_url}/{uri}'


    def _historical_indices_url(self, index, start, end):
        uri = f'historicalindices.jsp?indexType={urllib.parse.quote(index)}'
        uri += f'&fromDate={self._strfdate(start)}&toDate={self._strfdate(end)}'
        return f'{self._base_url}/{uri}'


    def build(self, resource: NseResource, index: NseIndex, start: date, end: date):
        # examples: historicalindices/NIFTY_50/2019
        if resource == 'historical_pepb':
            return self._pepb_values_url(index, start, end)
        elif resource == 'historicalindices':
            return self._historical_indices_url(index, start, end)
        else:
            raise ValueError(f'Unknown resource "{resource}"')



class CsvContentParser:

    @staticmethod
    def _row_parser(row):
        # split into words
        _words = row.split(',')
        # remove first and last double quote and whitespaces of each word
        _words = [w[1:-1].strip() for w in _words]
        return tuple(_words)


    def parse(self, content):
        # Split content into rows
        _rows = content.split(':')        
        # remove last row if empty
        if len(_rows[-1].strip()) == 0:
            _rows = _rows[:-1]
        
        # each rows to data object
        return list(map(self._row_parser, _rows))



class NseIndia:

    _headers = {
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7'
    }


    def __init__(self):
        self._url_builder = UrlBuilder()
        self._csv_content_parser = CsvContentParser()


    def _get_html(self, url):
        print('Open Url', url)
        _req = urllib.request.Request(url, None, headers=self._headers)
        with urllib.request.urlopen(_req) as _resp:
            _html = gzip.decompress(_resp.read())
        
        return str(_html, 'utf-8')


    @staticmethod
    def _find_csv_content(html):
        _soup = BeautifulSoup(html, 'html.parser')
        _csv_content = _soup.find('div', {"id": "csvContentDiv"})
        return _csv_content.getText()


    def get_data(self, resource: NseResource, index: NseIndex, start: date, end: date):
        _url = self._url_builder.build(resource, index, start, end)
        _html = self._get_html(_url)
        _content = self._find_csv_content(_html)
        _data = self._csv_content_parser.parse(_content)
        return _data


def get_nifty50_price_hist(start: date, end: date):
    return NseIndia().get_data(NseResource.PRICES, NseIndex.NIFTY_50, start, end)


def get_nifty50_pepb_hist(start: date, end: date):
    return NseIndia().get_data(NseResource.PEPB, NseIndex.NIFTY_50, start, end)


def get_nifty_next50_price_hist(start: date, end: date):
    return NseIndia().get_data(NseResource.PRICES, NseIndex.NIFTY_NEXT_50, start, end)


def get_nifty_next50_pepb_hist(start: date, end: date):
    return NseIndia().get_data(NseResource.PEPB, NseIndex.NIFTY_NEXT_50, start, end)


def to_df(data: list) -> pd.DataFrame:
    _df = pd.DataFrame.from_records(data[1:], columns=data[0])
    _df[_df.columns[1:]] = _df[_df.columns[1:]].apply(pd.to_numeric)
    _df['Date'] = pd.to_datetime(_df['Date'])
    return _df

