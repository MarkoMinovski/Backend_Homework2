from flask import Flask, jsonify, redirect
from DBClient import database as db
from scraper.latest_date_scraper_web import Latestdatescraper as lds
from scraper.web_scraper_main import web_scraper as ws

DEMO_LIMIT = 5

LATEST_AVAILABLE_DATE = lds.get_latest_available_date()

app = Flask(__name__)


# This is hard coded to only work for the "tickers" collection in our MongoDB database
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
        "BEST_turnover": BSON_table_row["BEST_turnover"]
    }


@app.route('/', methods=["GET"])
def default_route_handler():
    return redirect('/all'), 301


@app.route('/tickers/', methods=["GET"])
def redirect_wrong_access():
    return redirect('/all'), 301


@app.route('/all', methods=["GET"])
def get_all_tickers_route_handler():  # put application's code here
    tickers_all = db["tickers"].find()
    ret_json = []

    for doc in tickers_all:
        ret_json.append(convert_BSON_to_JSON_doc(doc))

    return jsonify(ret_json), 200


@app.route('/tickers/<ticker_id>')
def get_data_for_ticker(ticker_id: str):
    ticker_info_doc = db["tickers"].find_one({"ticker": ticker_id})

    if ticker_info_doc is None:
        return 404

    if ticker_info_doc["last_date_info"] < LATEST_AVAILABLE_DATE:
        while True:
            res = ws.main_scraping_loop()
            if res == "OK":
                break

    ret_json = []
    documents = db[ticker_id].find().sort("date", -1).limit(DEMO_LIMIT)

    for doc in documents:
        ret_json.append(convert_table_row_BSON_to_JSON(doc))

    return jsonify(ret_json), 200


if __name__ == '__main__':
    app.run()
