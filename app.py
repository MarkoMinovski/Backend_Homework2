import time
from flask import Flask, jsonify, redirect
from DBClient import database as db
from scraper.latest_date_scraper_web import Latestdatescraper as lds
from scraper.web_scraper_main import web_scraper as ws
import threading

DEMO_LIMIT = 5

LATEST_AVAILABLE_DATE = lds.get_latest_available_date()
START_TIME = time.time()

scraper_thread = None


def get_hours_uptime(seconds_uptime: float):
    return int(seconds_uptime) // 3600


LAST_HOURS_UPTIME_RESULT = 0

app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'


def convert_BSON_to_JSON_doc(BSON_obj):
    return {
        "ticker": BSON_obj["ticker"],
        "last_date_info": BSON_obj["last_date_info"]
    }


def convert_table_row_BSON_to_JSON(BSON_table_row):
    return {
        "date": BSON_table_row["date"],
        "date_str": BSON_table_row["date_str"],
        "last_trade_price": BSON_table_row["last_trade_price"],
        "max": BSON_table_row["max"],
        "min": BSON_table_row["min"],
        "avg": BSON_table_row["avg"],
        "percentage_change_decimal": BSON_table_row["percentage_change_decimal"],
        "vol": BSON_table_row["vol"],
        "BEST_turnover": BSON_table_row["BEST_turnover"],
        "total_turnover": BSON_table_row["total_turnover"]
    }


@app.before_request
def initiate_first_request_scrape():
    app.before_request_funcs[None].remove(initiate_first_request_scrape)
    initiate_scraper_thread()


def initiate_scraper_thread():
    global scraper_thread

    if scraper_thread is None or not scraper_thread.is_alive():
        scraper_thread = threading.Thread(target=thread_scraping_wrapper_func, daemon=True)
        print(f"Running background scrape on thread {str(scraper_thread)}")
        scraper_thread.start()


def thread_scraping_wrapper_func():
    try:
        ws.main_scraping_loop()
    finally:
        global scraper_thread
        print(f"Successful background scrape on thread {str(scraper_thread)}. Shutting down")
        scraper_thread = None


@app.route('/', methods=["GET"])
def default_route_handler():
    redirect_order = redirect('/all')
    redirect_order.headers.add('Access-Control-Allow-Origin', 'http://localhost:5173')
    return redirect_order, 301


@app.route('/tickers/', methods=["GET"])
def redirect_wrong_access():
    redirect_order = redirect('/all')
    redirect_order.headers.add('Access-Control-Allow-Origin', 'http://localhost:5173')
    return redirect_order, 301


@app.route('/tickers/latest', methods=["GET"])
def return_latest_trade_date():
    resp = LATEST_AVAILABLE_DATE
    resp = jsonify(resp)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp, 200


@app.route('/all', methods=["GET"])
def get_all_tickers_route_handler():  # put application's code here
    tickers_all = db["tickers"].find()
    current_time = time.time()
    current_uptime_res = get_hours_uptime(current_time)
    hour_has_passed = False

    global scraper_thread
    global LAST_HOURS_UPTIME_RESULT

    if current_uptime_res != LAST_HOURS_UPTIME_RESULT:
        hour_has_passed = True
        LAST_HOURS_UPTIME_RESULT = current_uptime_res

    if scraper_thread is None and hour_has_passed:
        initiate_scraper_thread()

    ret_json = []

    for doc in tickers_all:
        ret_json.append(convert_BSON_to_JSON_doc(doc))

    ret_json = jsonify(ret_json)
    ret_json.headers.add('Access-Control-Allow-Origin', 'http://localhost:5173')

    if scraper_thread is not None:
        if scraper_thread.is_alive():
            ret_json.headers.add("New info available", "True")

    return ret_json, 200


@app.route('/tickers/<ticker_id>')
def get_data_for_ticker(ticker_id: str):
    ticker_info_doc = db["tickers"].find_one({"ticker": ticker_id})

    if ticker_info_doc is None:
        return 404

    if ticker_info_doc["last_date_info"] < LATEST_AVAILABLE_DATE:
        ws.scrape_for_single_ticker(ticker_id)

    ret_json = []
    documents = db[ticker_id].find().sort("date", -1).limit(DEMO_LIMIT + 5)

    for doc in documents:
        ret_json.append(convert_table_row_BSON_to_JSON(doc))

    ret_json = jsonify(ret_json)
    ret_json.headers.add('Access-Control-Allow-Origin', 'http://localhost:5173')
    return ret_json, 200


if __name__ == '__main__':
    app.run()
