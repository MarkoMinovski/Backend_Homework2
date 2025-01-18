from abc import ABC, abstractmethod
import requests
from bs4 import BeautifulSoup

from scraper_refactored.auxiliary_functions.helper_functions import filter_result


# Utilizing the "template" design pattern from course material.
# https://refactoring.guru/design-patterns/template-method

class scraping_algorithm_base(ABC):
    @abstractmethod
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
    def scrape_batch(self):
        pass

    @abstractmethod
    def writeln(self):
        pass
