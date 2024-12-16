from pymongo.errors import BulkWriteError
from DBClient import database as db
import requests
from bs4 import BeautifulSoup
from scraper.tablerow import TableRow
from datetime import datetime, timedelta
from scraper.latest_date_scraper_web import Latestdatescraper

DEFAULT_URL = "https://www.mse.mk/en/stats/symbolhistory/MPT"
TODAY = datetime.today()
LATEST_AVAILABLE = Latestdatescraper.get_latest_available_date()


def is_less_than_year_ago(date):
    one_year_ago = TODAY - timedelta(days=364)
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


class Tablescraper:
    @staticmethod
    def scrape_table(ticker_code: str, latest_date, write_locally, file, writer):
        finished_batch = False
        no_table_in_previous_cycle = False
        search_date = latest_date
        date_return_value = latest_date

        while not finished_batch:

            if no_table_in_previous_cycle:
                search_date += timedelta(weeks=8)
                if search_date >= LATEST_AVAILABLE:
                    finished_batch = True

            successful_response = False
            response = None

            while successful_response is False:
                print(f"Sending POST request for ticker {ticker_code}")
                response = Tablescraper.send_post_request(ticker_code, search_date)
                if response == None:
                    print("Encountered non-200 HTTP Status return code. Retrying request")
                else:
                    successful_response = True

            soup = BeautifulSoup(response.content, "html.parser")

            table_rows = soup.find_all('tr')

            if not table_rows:
                print(f"Lack of info for current time period. Pushing ahead by 8 weeks!")
                no_table_in_previous_cycle = True
                continue

            print("Table rows in HTML found")

            table_ticker_collection = db[ticker_code]
            all_rows_to_be_written_list = []

            for row in table_rows:
                children = row.find_all("td", recursive=False)

                if len(children) != 9:
                    continue

                table_row_obj = TableRow()

                table_row_obj.date = children[0].text
                table_row_obj.last_trade_price = children[1].text
                table_row_obj.max = children[2].text
                table_row_obj.min = children[3].text
                table_row_obj.avg = children[4].text
                table_row_obj.percentage_change_as_decimal = children[5].text
                table_row_obj.volume = children[6].text
                table_row_obj.BEST_turnover_in_denars = children[7].text
                table_row_obj.total_turnover_in_denars = children[8].text

                d_m_y = get_day_month_year(table_row_obj.date)
                datetime_d_m_y = datetime(int(d_m_y[2]), int(d_m_y[0]), int(d_m_y[1]))

                if write_locally is False:
                    table_row_obj = reformat_delimiters(table_row_obj)

                    row_doc = {
                        "date": datetime_d_m_y,
                        "date_str": table_row_obj.date,
                        "last_trade_price": table_row_obj.last_trade_price,
                        "max": table_row_obj.max,
                        "min": table_row_obj.min,
                        "avg": table_row_obj.avg,
                        "percentage_change_decimal": table_row_obj.percentage_change_as_decimal,
                        "vol": table_row_obj.volume,
                        "BEST_turnover": table_row_obj.BEST_turnover_in_denars,
                        "total_turnover": table_row_obj.total_turnover_in_denars
                    }

                    all_rows_to_be_written_list.append(row_doc)
                else:
                    row_csv_raw = {
                        "code": ticker_code,
                        "date": table_row_obj.date,
                        "last_trade_price": table_row_obj.last_trade_price,
                        "max": table_row_obj.max,
                        "min": table_row_obj.min,
                        "avg": table_row_obj.avg,
                        "percentage_change": table_row_obj.percentage_change_as_decimal,
                        "volume": table_row_obj.volume,
                        "best_turnover": table_row_obj.BEST_turnover_in_denars,
                        "total_turnover": table_row_obj.total_turnover_in_denars
                    }

                    writer.writerow(row_csv_raw)
                    if datetime_d_m_y > date_return_value:
                        date_return_value = datetime_d_m_y

            if write_locally is False:
                try:
                    table_ticker_collection.insert_many(all_rows_to_be_written_list)
                except BulkWriteError as bwe:
                    print(bwe.details)

            finished_batch = True

        print("Writing complete")

        # Only used if writing to local file
        return date_return_value

    @staticmethod
    def send_post_request(ticker_code, latest_date):
        from_date = latest_date
        if is_less_than_year_ago(from_date):
            to_date = LATEST_AVAILABLE
        else:
            to_date = from_date + timedelta(days=364)

        header = {
            "content_type": "application/x-www-form-urlencoded"
        }

        from_date_string = str(from_date.month) + "/" + str(from_date.day) + "/" + str(from_date.year)
        to_date_string = str(to_date.month) + "/" + str(to_date.day) + "/" + str(to_date.year)

        print(f"Building POST request with FromDate {from_date_string}, ToDate {to_date_string} and CODE {ticker_code}")

        payload = {
            "FromDate": from_date_string,
            "ToDate": to_date_string,
            "Code": ticker_code
        }
        server_resp = requests.post(DEFAULT_URL, headers=header, data=payload)

        if server_resp.status_code == 200:
            return server_resp
        else:
            return None
