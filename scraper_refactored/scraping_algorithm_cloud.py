from datetime import timedelta, datetime

from pymongo.errors import BulkWriteError

from scraper.tablerow import TableRow
from scraper_refactored.scraping_algorithm_base_class import scraping_algorithm_base as base
from scraper_refactored.auxiliary_functions.helper_functions import get_latest_available_date, get_day_month_year, \
    reformat_delimiters, build_ancillary_status_list
from bs4 import BeautifulSoup
from DBClient import database as db


class scraping_algorithm_cloud(base):

    def __init__(self, range_start, scraping_url):
        super().__init__(range_start, scraping_url)

    def build_status_pairs(self, ticker_codes):
        ticker_name_last_date_pairs = []
        ticker_info_collection = db["tickers"]

        for ticker in ticker_codes:
            query_result = ticker_info_collection.find_one({"ticker": ticker})

            if query_result is None:
                new_doc = {
                    "ticker": ticker,
                    "last_date_info": self.range_start
                }

                ticker_info_collection.insert_one(new_doc)
                ticker_name_last_date_pairs.append((ticker, self.range_start))
            else:
                tmp_tuple = (query_result["ticker"], query_result["last_date_info"])
                ticker_name_last_date_pairs.append(tmp_tuple)
        return ticker_name_last_date_pairs

    def execute_main_loop(self):
        main_page = "https://www.mse.mk/en"

        print("Gathering tickers")

        # Filter I
        ticker_codes = self.gather_eligible_tickers(initial_url=self.scraping_url)

        print("Gathering complete")

        print("Building pairs list")

        # Filter II
        name_last_date_pairs = self.build_status_pairs(ticker_codes=ticker_codes)

        print("Pairs list built")

        print("Beginning loop")

        lda = get_latest_available_date(main_page)

        print("Building statuses list for each pair")

        statuses = build_ancillary_status_list(ticker_pairs=name_last_date_pairs, latest_available_date=lda)

        print("Beginning scrape!")
        # Filter III
        self.scrape_for_all(pairs=name_last_date_pairs, statuses=statuses, lda=lda)
        print("Scrape finished!")

    def scrape_for_all(self, pairs, statuses, lda):
        last_pos = -1
        iterations_on_current_position = 0
        ticker_info_collection = db["tickers"]
        while any(status is False for status in statuses):
            current_pos = statuses.index(False)
            next_outdated_ticker_pos = current_pos
            next_outdated_ticker = pairs[next_outdated_ticker_pos]

            # without this check, the end of available data is eventually reached, but the code for checking that is
            # inaccessible
            if next_outdated_ticker[1].date() == lda.date():
                statuses[current_pos] = True
                continue

            print(f"Calling Tablescraper for ticker with code {next_outdated_ticker[0]}")
            print(f"... with latest available date {next_outdated_ticker[1]}")

            # Search from the next day!
            (ret_date, scraped_list) = (
                self.scrape_batch(next_outdated_ticker[0], next_outdated_ticker[1] + timedelta(days=1), lda))

            print(f"Scraping batch for ticker {next_outdated_ticker[0]} successful")

            if len(scraped_list) > 0:
                print("Writing results")
                self.writeln(scraped_list, next_outdated_ticker[0])

            # after scraping one batch (364 days worth)...
            latest_available_after_scraping = db[next_outdated_ticker[0]].find().sort("date", -1)[0]
            date_latest_for_current_ticker = latest_available_after_scraping["date"]

            print(f"Updating {next_outdated_ticker[0]} latest date in ticker info collection")

            ticker_info_collection.update_one(
                {"ticker": next_outdated_ticker[0]},
                {"$set": {"last_date_info": date_latest_for_current_ticker}}
            )

            # rebuild pair with new date, to not get stuck in an infinite loop
            new_pair_values = (next_outdated_ticker[0], date_latest_for_current_ticker)
            pairs[next_outdated_ticker_pos] = new_pair_values

            if date_latest_for_current_ticker.date() == lda.date():
                statuses[current_pos] = True

            # Realistically this "and ret != latest_available_date.date()" is unnecessary but
            # just in case... The idea with this magic 11 is that on a successful scrape we scrape an entire year,
            # so it should never take more than 11 iterations
            elif iterations_on_current_position > 11 and ret_date != lda.date():
                statuses[current_pos] = True

            if last_pos == current_pos:
                iterations_on_current_position += 1
            else:
                iterations_on_current_position = 0
            last_pos = current_pos

    def scrape_batch(self, ticker_code, latest_date_in_collection, lda):
        finished_batch = False
        no_table_in_previous_cycle = False
        search_date = latest_date_in_collection
        date_return_value = latest_date_in_collection

        while not finished_batch:

            if no_table_in_previous_cycle:
                search_date += timedelta(weeks=8)
                if search_date >= lda:
                    finished_batch = True

            successful_response = False
            response = None

            while successful_response is False:
                print(f"Sending POST request for ticker {ticker_code}")

                # Method inherited from abstract parent class!
                response = self.send_post_request_for(ticker_code, search_date, lda)
                if response is None:
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

                if datetime_d_m_y > date_return_value:
                    date_return_value = datetime_d_m_y

            finished_batch = True

        print("Writing complete")

        # Returns the latest scraped date in the loop
        return date_return_value, all_rows_to_be_written_list

    def writeln(self, scraped_list, ticker_code):

        ticker_collection_individual = db[ticker_code]

        not_successful = True

        while not_successful:
            try:
                ticker_collection_individual.insert_many(scraped_list)
                not_successful = False
            except BulkWriteError as bwe:
                print(bwe.details)
                not_successful = True
            except Exception as e:
                print("Unknown exception:")
                print(e)
                not_successful = True
