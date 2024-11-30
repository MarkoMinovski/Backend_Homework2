import csv
import sys

from datetime import datetime, timedelta
from scraper.ticker_scraper_web import TickerScraper
from DBClient import database as db
from scraper.table_scraper_web import Tablescraper
from scraper.latest_date_scraper_web import Latestdatescraper
import time

TEN_YEARS_PRIOR = datetime.today() - timedelta(days=(365 * 10) - 1)
# START = time.time()
# END = None


class web_scraper:

    @staticmethod
    def main_scraping_loop():
        user_input = "N"

        if user_input == "Y" or user_input == "y":
            local_write = True
        elif user_input == "N" or user_input == "n":
            local_write = False
        else:
            print("Unrecognized input. Defaulting to Database write")
            local_write = False

        initial_url = "https://www.mse.mk/en/stats/symbolhistory/MPT"
        latest_available_date = Latestdatescraper.get_latest_available_date()
        raw_tickers_scraper = TickerScraper(initial_url)

        print("Scraping list of tickers from HTML...")

        # First part of the pipeline
        tickers_filtered = raw_tickers_scraper.initial_scrape()

        if tickers_filtered is None:
            print("Error in the first part of the pipeline")
            sys.exit(1)

        # existing_tickers = db.list_collection_names()
        ticker_name_last_date_pairs = []
        # list of tuples, with ticker name and last available date as the two elements

        # We have a collection in our database that holds information about each ticker and the latest date for
        # which we have scraped data
        ticker_info_collection = db["tickers"]

        print("Building ticker info pairs...")

        # Second part of the pipeline
        for ticker in tickers_filtered:

            if local_write is False:
                query_result = ticker_info_collection.find_one({"ticker": ticker})

                if query_result is None:

                    new_doc = {
                        "ticker": ticker,
                        "last_date_info": TEN_YEARS_PRIOR
                    }

                    ticker_info_collection.insert_one(new_doc)
                    ticker_name_last_date_pairs.append((ticker, TEN_YEARS_PRIOR))

                else:
                    tmp_tuple = (query_result["ticker"], query_result["last_date_info"])
                    ticker_name_last_date_pairs.append(tmp_tuple)
            else:
                ticker_name_last_date_pairs.append((ticker, TEN_YEARS_PRIOR))

        # TODO: TRANSFORM ABOVE CODE TO BE MORE MODULAR, AND EXPAND IT

        # third part of the pipeline

        # this list will be a list of additional boolean values for each index of the tuples list above. example:
        # is_up_to_date[0] = True
        # means the ticker_last_date pair is up-to-date, and we don't need to scrape for them

        print("Building status list...")
        is_up_to_date = []

        # first we need to build this list
        for ticker_name_last_date_pair in ticker_name_last_date_pairs:
            current_ticker_date = ticker_name_last_date_pair[1]

            if current_ticker_date.date() != latest_available_date.date():
                is_up_to_date.append(False)
            else:
                is_up_to_date.append(True)

        print("Entering main LOOP")

        if local_write is True:
            file_str = "raw_output_" + str(datetime.today().date())
            file = open(file_str, mode="w", newline="")
            headers = ["code", "date", "last_trade_price", "max", "min", "avg", "percentage_change", "volume",
                       "best_turnover", "total_turnover"]
            writer = csv.DictWriter(file, fieldnames=headers)
        else:
            file = None
            writer = None

        while any(status is False for status in is_up_to_date):
            current_pos = is_up_to_date.index(False)
            next_outdated_ticker_pos = current_pos
            next_outdated_ticker = ticker_name_last_date_pairs[next_outdated_ticker_pos]

            # without this check, the end of available data is eventually reached, but the code for checking that is
            # inaccessible
            if next_outdated_ticker[1].date() == latest_available_date.date():
                is_up_to_date[current_pos] = True
                continue

            print(f"Calling Tablescraper for ticker with code {next_outdated_ticker[0]}")
            print(f"... with latest available date {next_outdated_ticker[1]}")

            # Search from the next day!
            ret = Tablescraper.scrape_table(next_outdated_ticker[0], next_outdated_ticker[1] + timedelta(days=1),
                                            local_write, file, writer)

            print(f"Scraping batch for ticker {next_outdated_ticker[0]} successful")

            if local_write is False:
                # after scraping one batch (364 days worth)...
                latest_available_after_scraping = db[next_outdated_ticker[0]].find().sort("date", -1)[0]
                date_latest_for_current_ticker = latest_available_after_scraping["date"]

                print(f"Updating {next_outdated_ticker[0]} latest date in ticker info collection")

                ticker_info_collection_doc_for_current_ticker = ticker_info_collection.update_one(
                    {"ticker": next_outdated_ticker[0]},
                    {"$set": {"last_date_info": date_latest_for_current_ticker}}
                )
            else:
                date_latest_for_current_ticker = ret

            # rebuild pair with new date, to not get stuck in an infinite loop
            new_pair_values = (next_outdated_ticker[0], date_latest_for_current_ticker)
            ticker_name_last_date_pairs[next_outdated_ticker_pos] = new_pair_values

            if date_latest_for_current_ticker.date() == latest_available_date.date():
                is_up_to_date[current_pos] = True

        if local_write:
            file.close()

        return "OK"
