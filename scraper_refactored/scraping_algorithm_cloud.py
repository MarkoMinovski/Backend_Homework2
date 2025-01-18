from scraper_refactored.scraping_algorithm_base_class import scraping_algorithm_base as base
import requests
from bs4 import BeautifulSoup
from DBClient import database as db


class scraping_algorithm_cloud(base):

    def gather_eligible_tickers(self):
        pass
