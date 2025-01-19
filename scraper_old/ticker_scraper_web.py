import requests
from bs4 import BeautifulSoup

HTTP_STATUS_OK = 200


# The first filter as defined by the Homework I document.
def filter_result(all_tickers_set):
    tickers_filtered = []

    for ticker in all_tickers_set:
        if any(character.isdigit() for character in ticker):
            continue
        else:
            tickers_filtered.append(ticker)

    return tickers_filtered


class TickerScraper:
    def __init__(self, initial_url):
        self.initial_url = initial_url

    # Scrape tickers
    # Though we will remove government bonds and codes that contain numbers here and now. Rest, just scrape and send
    # for later.
    def initial_scrape(self):
        server_response = requests.get(self.initial_url)

        print("Server responded to initial HTTP request")

        if server_response.status_code == HTTP_STATUS_OK:
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

    # Remove tickers with numbers and government bonds
