from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup


def filter_result(all_tickers_set):
    tickers_filtered = []

    for ticker in all_tickers_set:
        if any(character.isdigit() for character in ticker):
            continue
        else:
            tickers_filtered.append(ticker)

    return tickers_filtered


def get_latest_available_date(url):
    resp = requests.get(url)
    latest_m_d_y_datetime = None

    print("Finding latest available date...")

    # ok
    if resp.status_code == 200:
        soup = BeautifulSoup(resp.content, "html.parser")
        target_div = soup.find("div", id="topSymbolValueTopSymbols")
        child_div_containing_latest_info = target_div.find_all("div", recursive=False)[0]
        latest_month_day_year = child_div_containing_latest_info.text.split("/")
        latest_m_d_y_datetime = datetime(int(latest_month_day_year[2]), int(latest_month_day_year[0]),
                                         int(latest_month_day_year[1]))

    print(f"Latest available date is {latest_m_d_y_datetime}")
    return latest_m_d_y_datetime


def get_latest_available_date_as_string(url):
    resp = requests.get(url)
    ret_string = ''

    print("Finding latest available date...")

    if resp.status_code == 200:
        soup = BeautifulSoup(resp.content, "html.parser")
        target_div = soup.find("div", id="topSymbolValueTopSymbols")
        child_div_containing_latest_info = target_div.find_all("div", recursive=False)[0]
        ret_string = child_div_containing_latest_info.text

    return ret_string


def is_less_than_year_ago(date):
    one_year_ago = datetime.today() - timedelta(days=364)
    if date < one_year_ago:
        return False
    else:
        return True


def reformat_delimiters(table_row_object):
    tmp = table_row_object
    tmp.max = reformat_price_delimiter(table_row_object.max)
    tmp.min = reformat_price_delimiter(table_row_object.min)
    tmp.avg = reformat_price_delimiter(table_row_object.avg)
    tmp.last_trade_price = reformat_price_delimiter(table_row_object.last_trade_price)

    return tmp


def reformat_price_delimiter(price_string: str):
    tmp_price_str = price_string
    tmp_price_str = tmp_price_str.replace(",", ".")
    split = tmp_price_str.rsplit(".", 1)
    tmp_price_str = ",".join(split)
    return tmp_price_str


def get_day_month_year(date: str):
    day_m_year_list = date.split("/")
    return day_m_year_list


def build_ancillary_status_list(ticker_pairs, latest_available_date):
    is_up_to_date = []
    for ticker_name_last_date_pair in ticker_pairs:
        current_ticker_date = ticker_name_last_date_pair[1]

        if current_ticker_date.date() != latest_available_date.date():
            is_up_to_date.append(False)
        else:
            is_up_to_date.append(True)

    return is_up_to_date
