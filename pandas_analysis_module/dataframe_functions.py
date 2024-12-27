import pandas as pd
import datetime
from DBClient import database as db


def get_documents_in_range(ticker_code: str, range_earliest: datetime, range_latest: datetime):
    query_filter = {
        "date": {
            "$gte": range_earliest,
            "$lte": range_latest
        }
    }

    res_cursor = db[ticker_code].find(query_filter)

    # Very expensive
    return list(res_cursor)


def convert_to_float(number_str: str):
    number_str = number_str.replace(",", ".")

    num_without_periods = number_str.rsplit(".", 1)

    if len(num_without_periods) >= 2:
        val = num_without_periods[0].replace('.', '') + '.' + num_without_periods[1]
    else:
        val = num_without_periods[0]

    return float(val)


def convert_fields_to_numeric_types(single_document_obj):
    if single_document_obj["vol"] == "0":
        new_dict = {
            "date": single_document_obj["date"],
            "date_str": single_document_obj["date_str"],
            "last_trade_price": convert_to_float(single_document_obj["last_trade_price"]),
            "max": 0,
            "min": 0,
            "avg": convert_to_float(single_document_obj["avg"]),
            "vol": convert_to_float(single_document_obj["vol"]),
            "BEST_turnover": convert_to_float(single_document_obj["BEST_turnover"]),
            "total_turnover": convert_to_float(single_document_obj["total_turnover"])
        }
    else:
        new_dict = {
            "date": single_document_obj["date"],
            "date_str": single_document_obj["date_str"],
            "last_trade_price": convert_to_float(single_document_obj["last_trade_price"]),
            "max": convert_to_float(single_document_obj["max"]),
            "min": convert_to_float(single_document_obj["min"]),
            "avg": convert_to_float(single_document_obj["avg"]),
            "vol": convert_to_float(single_document_obj["vol"]),
            "BEST_turnover": convert_to_float(single_document_obj["BEST_turnover"]),
            "total_turnover": convert_to_float(single_document_obj["total_turnover"])
        }
    return new_dict


def create_dataframe(ticker_code: str, range_earliest: datetime, range_latest: datetime):
    docs_list = get_documents_in_range(ticker_code, range_earliest, range_latest)

    for i in range(len(docs_list)):
        docs_list[i] = convert_fields_to_numeric_types(docs_list[i])

    df = pd.DataFrame(docs_list)

    print(df)

    return df
