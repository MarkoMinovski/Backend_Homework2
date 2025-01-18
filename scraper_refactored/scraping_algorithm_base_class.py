from abc import ABC, abstractmethod
from datetime import timedelta

import requests
from bs4 import BeautifulSoup

from scraper_refactored.auxiliary_functions.helper_functions import filter_result
from scraper_refactored.auxiliary_functions.helper_functions import is_less_than_year_ago


# Utilizing the "template" design pattern from course material.
# https://refactoring.guru/design-patterns/template-method

class scraping_algorithm_base(ABC):

    def __init__(self, range_start, scraping_url):
        self.range_start = range_start
        self.scraping_url = scraping_url

    def gather_eligible_tickers(self, initial_url):
        server_response = requests.get(initial_url)

        print("Server responded to initial HTTP request")

        if server_response.status_code == 200:
            beautiful_soup_parser = BeautifulSoup(server_response.content, 'html.parser')

            print("Server response OK")

            # all tickers
            select_tag = beautiful_soup_parser.find('select', id='Code')

            if select_tag is not None:
                tickers_res_set = select_tag.find_all('option')

                tickers_values = [ticker['value'] for ticker in tickers_res_set]

                filtered_tickers_list = filter_result(tickers_values)
            else:
                return None
        else:
            return None

        print("Result of ticker scraping:")
        print(filtered_tickers_list)
        print("=======================")
        return filtered_tickers_list

    @abstractmethod
    def build_status_pairs(self, ticker_codes):
        pass

    @abstractmethod
    def execute_main_loop(self):
        pass

    @abstractmethod
    def scrape_batch(self, ticker_code, latest_date_in_collection, lda):
        pass

    @abstractmethod
    def writeln(self, scraped_collection, ticker_code):
        pass

    def send_post_request_for(self, ticker_code, latest_date_in_collection, lda):
        from_date = latest_date_in_collection
        if is_less_than_year_ago(from_date):
            to_date = lda
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
        server_resp = requests.post(self.scraping_url, headers=header, data=payload)

        if server_resp.status_code == 200:
            return server_resp
        else:
            return None
