import time
import datetime
from flask import Flask, jsonify, redirect, request
from DBClient import database as db
# old
from scraper.latest_date_scraper_web import Latestdatescraper as lds
from scraper.web_scraper_main import web_scraper as ws
from scraper.table_scraper_web import get_day_month_year

# new
from scraper_refactored.scraping_algorithm_cloud import scraping_algorithm_cloud as scraping_algorithm_cloud_template
from scraper_refactored.auxiliary_functions.helper_functions import get_ten_years_ago

import pandas as pd
import ta

from pandas_analysis_module.dataframe_functions import create_dataframe

import threading

DEMO_LIMIT = 5

STARTER_URL = "https://www.mse.mk/en/stats/symbolhistory/ALK"

LATEST_AVAILABLE_DATE = lds.get_latest_available_date()
START_TIME = time.time()
FRONTEND_URL = '*'

scraper_obj = scraping_algorithm_cloud_template(get_ten_years_ago(),
                                                STARTER_URL)
scraper_thread = None


def split_periods_string(string: str):
    return string.split('.')


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
        scraper_obj.execute_main_loop()
    finally:
        global scraper_thread
        print(f"Successful background scrape on thread {str(scraper_thread)}. Shutting down")
        scraper_thread = None


@app.route('/', methods=["GET"])
def default_route_handler():
    redirect_order = redirect('/all')
    redirect_order.headers.add('Access-Control-Allow-Origin', FRONTEND_URL)
    return redirect_order, 301


@app.route('/tickers/latest/str', methods=["GET"])
def return_latest_trade_date_as_str():
    resp = lds.get_latest_available_date_as_string()
    resp = jsonify(resp)
    resp.headers.add('Access-Control-Allow-Origin', FRONTEND_URL)
    return resp, 200


@app.route('/tickers/', methods=["GET"])
def redirect_wrong_access():
    redirect_order = redirect('/all')
    redirect_order.headers.add('Access-Control-Allow-Origin', FRONTEND_URL)
    return redirect_order, 301


@app.route('/tickers/latest', methods=["GET"])
def return_latest_trade_date():
    resp = LATEST_AVAILABLE_DATE
    resp = jsonify(resp)
    resp.headers.add('Access-Control-Allow-Origin', FRONTEND_URL)
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
    ret_json.headers.add('Access-Control-Allow-Origin', FRONTEND_URL)

    if scraper_thread is not None:
        ret_json.headers.add("New info available", "True")

    return ret_json, 200


@app.route('/tickers/<ticker_id>')
def get_data_for_ticker(ticker_id: str):
    # DO NOT USE
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
    return ret_json, 200


@app.route('/tickers/range/<ticker_id>')
def get_date_range_for_ticker(ticker_id: str):
    ticker_info_doc = db["tickers"].find_one({"ticker": ticker_id})
    ticker_name = ticker_info_doc["ticker"]
    ticker_earliest_doc = db[ticker_name].find().sort("date", 1).limit(1)
    ticker_earliest_date = ticker_earliest_doc[0]["date"]
    ticker_latest_date = ticker_info_doc["last_date_info"]

    ret_json = {
        "code": ticker_name,
        "date_earliest": ticker_earliest_date,
        "date_latest": ticker_latest_date
    }

    ret_json = jsonify(ret_json)
    ret_json.headers.add('Access-Control-Allow-Origin', FRONTEND_URL)

    return ret_json, 200


@app.route('/tickers/analyze', methods=['POST'])
def accept_analysis_post():
    # DO NOT USE
    ticker_code = request.form.get("code")
    date_start_list = get_day_month_year(request.form.get("interval_start"))
    date_end_list = get_day_month_year(request.form.get("interval_end"))
    analysis_type = request.form.get("analysis_type")

    date_start_in_datetime = datetime.datetime(int(date_start_list[2]), int(date_start_list[1]),
                                               int(date_start_list[0]))
    date_end_in_datetime = datetime.datetime(int(date_end_list[2]), int(date_end_list[1]), int(date_end_list[0]))

    df = create_dataframe(ticker_code=ticker_code, range_earliest=date_start_in_datetime,
                          range_latest=date_end_in_datetime)

    days_in_interval = date_end_in_datetime - date_start_in_datetime

    print(days_in_interval)

    res = []

    if analysis_type == "rolling_average":
        sma_ltp = df['last_trade_price'].rolling(window=int(days_in_interval.days), min_periods=1).mean()
        sma_ltp.fillna('0')
        sma_ltp.name = "last_trade_price_SMA"

        res.append(sma_ltp)

        ema_ltp = df['last_trade_price'].ewm(span=days_in_interval.days, min_periods=1).mean()
        ema_ltp.fillna('0')
        ema_ltp.name = "last_trade_price_EMA"

        res.append(ema_ltp)

        cma_ltp = df['last_trade_price'].expanding().mean()
        cma_ltp.fillna('0')
        cma_ltp.name = "last_trade_price_CMA"

        res.append(cma_ltp)

        ret = pd.concat(res, axis=1)

        ret = ret.to_json(orient='records')
    elif analysis_type == "oscillators":
        df_with_momentum_oscillators = (ta.add_momentum_ta
                                        (df=df, high='max', low='min', close='last_trade_price',
                                         volume='vol', fillna=True))

        print(df_with_momentum_oscillators)

        ret = df_with_momentum_oscillators.to_json(orient='records', force_ascii=False)

    return jsonify(ret)


@app.route('/tickers/analyze/averages/<interval_start>/<interval_end>/<ticker_code_param>', methods=['GET'])
def analyze_moving_averages(interval_start: str, interval_end: str, ticker_code_param: str):
    ticker_code = ticker_code_param
    date_start_list = split_periods_string(interval_start)
    date_end_list = split_periods_string(interval_end)

    date_start_in_datetime = datetime.datetime(int(date_start_list[2]), int(date_start_list[0]),
                                               int(date_start_list[1]))
    date_end_in_datetime = datetime.datetime(int(date_end_list[2]), int(date_end_list[0]), int(date_end_list[1]))

    df = create_dataframe(ticker_code=ticker_code, range_earliest=date_start_in_datetime,
                          range_latest=date_end_in_datetime)

    days_in_interval = date_end_in_datetime - date_start_in_datetime

    print(days_in_interval)

    res = []

    date_column = df['date']
    date_column.name = "date"

    res.append(date_column)

    sma_ltp = df['last_trade_price'].rolling(window=days_in_interval.days, min_periods=1).mean()
    sma_ltp.fillna(0)
    sma_ltp.name = "last_trade_price_SMA"

    res.append(sma_ltp)

    ema_ltp = df['last_trade_price'].ewm(span=days_in_interval.days, min_periods=1).mean()
    ema_ltp.fillna(0)
    ema_ltp.name = "last_trade_price_EMA"

    res.append(ema_ltp)

    cma_ltp = df['last_trade_price'].expanding().mean()
    cma_ltp.fillna(0)
    cma_ltp.name = "last_trade_price_CMA"

    res.append(cma_ltp)

    ret = pd.concat(res, axis=1)

    ret = ret.to_dict(orient='records')

    response = jsonify(ret)
    response.headers.add('Access-Control-Allow-Origin', FRONTEND_URL)

    return response


@app.route('/tickers/analyze/oscillators/<interval_start>/<interval_end>/<ticker_code_param>', methods=['GET'])
def oscillator_analysis(interval_start: str, interval_end: str, ticker_code_param: str):
    ticker_code = ticker_code_param
    date_start_list = split_periods_string(interval_start)
    date_end_list = split_periods_string(interval_end)

    date_start_in_datetime = datetime.datetime(int(date_start_list[2]), int(date_start_list[0]),
                                               int(date_start_list[1]))
    date_end_in_datetime = datetime.datetime(int(date_end_list[2]), int(date_end_list[0]), int(date_end_list[1]))

    df = create_dataframe(ticker_code=ticker_code, range_earliest=date_start_in_datetime,
                          range_latest=date_end_in_datetime)


    df_with_momentum_oscillators = (ta.add_momentum_ta
                                    (df=df, high='max', low='min', close='last_trade_price',
                                     volume='vol', fillna=True))

    df_as_dict = df_with_momentum_oscillators.to_dict(orient='records')

    response = jsonify(df_as_dict)

    response.headers.add('Access-Control-Allow-Origin', FRONTEND_URL)

    return response


if __name__ == '__main__':
    app.run()
