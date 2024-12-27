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

    # Very expensive! Thankfully, the list will be at most 365 records
    return list(res_cursor)


def create_dataframe(ticker_code: str, range_earliest: datetime, range_latest: datetime):
    docs_list = get_documents_in_range(ticker_code, range_earliest, range_latest)
    df = pd.DataFrame(docs_list)

    print(df)

    return df
