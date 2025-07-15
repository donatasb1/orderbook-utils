import pandas as pd
from datetime import datetime
import time
import concurrent.futures
from functools import partial
from orderbook.xmlgen.converter import split_and_write_xml
from orderbook.db.csvreader import OrderbookCSVA
import numpy as np


def _convert_period(period: tuple[datetime, datetime], path: str) -> int:
    """Made to work on adhoc folders"""
    start_date, end_date = period
    # SAB1L
    db = OrderbookCSVA("data/LT0000102253/")
    orders = db.fetch_filtered_orderbook_data(
        market="INET_MainMarket",
        start=start_date,
        end=end_date,
        tickers=['SAB1L'],
    )
    # NTU1L
    db = OrderbookCSVA("data/LT0000131872/")
    orders2 = db.fetch_filtered_orderbook_data(
        market="INET_MainMarket",
        start=start_date,
        end=end_date,
        tickers=['NTU1L'],
    )
    orders = pd.concat([orders, orders2], axis=0, ignore_index=True)
    if orders.shape[0] > 0:
        split_and_write_xml(orders, path=path, ver='001', cap=250000)
        return orders.shape[0]
    return 0

def convert_period(start: datetime, end: datetime, path: str = 'xml_output') -> str:
    """Create monthly XML files"""
    ms = pd.date_range(start=start, end=end, freq='MS')
    me = pd.date_range(start=start, end=end, freq='ME')
    start_ts = time.time()
    date_idx = pd.date_range(start=start, end=end, freq="B")
    n_threads = min(6, int(date_idx.shape[0] // 30 + 1))
    chunksize = max(1, int(np.ceil(date_idx.shape[0] / n_threads)))
    print(f"Using {n_threads} threads with chunksize {1} for processing {date_idx.shape[0]} dates.")
    with concurrent.futures.ProcessPoolExecutor(max_workers=n_threads) as executor:
        res = executor.map(partial(_convert_period, path=path), zip(ms, me), chunksize=1)
        res = [r for r in res if r is not None]

    end_ts = time.time()
    print(f"Total time taken: {end_ts - start_ts:.2f} seconds for processing {date_idx.shape[0]} dates, {sum(res)} rows.")

