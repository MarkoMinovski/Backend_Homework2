from datetime import datetime
import requests
from bs4 import BeautifulSoup

MAIN_PAGE_URL = "https://www.mse.mk/en"
HTTP_STATUS_OK = 200


class Latestdatescraper:
    @staticmethod
    def get_latest_available_date():
        resp = requests.get(MAIN_PAGE_URL)
        latest_m_d_y_datetime = None

        print("Finding latest available date...")

        if resp.status_code == HTTP_STATUS_OK:
            soup = BeautifulSoup(resp.content, "html.parser")
            target_div = soup.find("div", id="topSymbolValueTopSymbols")
            child_div_containing_latest_info = target_div.find_all("div", recursive=False)[0]
            latest_month_day_year = child_div_containing_latest_info.text.split("/")
            latest_m_d_y_datetime = datetime(int(latest_month_day_year[2]), int(latest_month_day_year[0]),
                                             int(latest_month_day_year[1]))

        print(f"Latest available date is {latest_m_d_y_datetime}")
        return latest_m_d_y_datetime
