import logging
from orderbook.utils import setup_logs
setup_logs()
from typing import List, Optional, Union
from collections import defaultdict
import functools
import concurrent.futures
import pandas as pd
from datetime import datetime
import os
from orderbook.MarketTypes import MARKET, PHASE
from orderbook.db.base import OrderbookDB, orderbook_cols

logger = logging.getLogger(__name__)


class OrderbookCSVA(OrderbookDB):
    """Reads csv files and returns a dataframe. Implements OrderbookDB interface.

    Attributes
    ----------
    root : str
        Path to the directory containing CSV files.

    Methods
    -------
    fetch_filtered_orderbook_data(market, start, end, tickers=None, phases=None): 
        Validates function arguments and returns filtered orderbook events.
    """
    

    def __init__(self, path: str):
        """
        Parameters
        ----------
        path : str
            Path to the directory containing CSV files.
        """
        self.root = path

    def fetch_filtered_orderbook_data(
        self,
        market: MARKET,
        start: datetime, 
        end: datetime,
        tickers: Optional[Union[str, List[str]]] = None,        
        phases: Optional[Union[PHASE, List[PHASE]]] = None,
    ) -> pd.DataFrame:
        """
        Validates function arguments and returns filtered orderbook events.

        Parameters
        ----------
        market : MARKET
            The market identifier, e.g., 'INET_MainMarket', 'INET_FirstNorth', etc.
        start : datetime
            The start datetime for filtering orderbook events.
        end : datetime
            The end datetime for filtering orderbook events.
        tickers : str or List[str], optional
            A single ticker or a list of tickers to filter the orderbook events. Defaults to None, which includes all tickers.
        phases : PHASE or List[PHASE], optional
            A single phase or a list of phases to filter the orderbook events. Defaults to None, which includes all phases.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing filtered orderbook events with additional calculated columns such as 'value' and 'ordervalue'.

        Raises
        ------
        AssertionError
            If the provided tickers or phases are invalid.
        FileNotFoundError
            If no data is found for the specified market, date range, tickers, and phases.
        """
        _tickers = tuple(tickers) if tickers is not None else None
        _phases = tuple(phases) if phases is not None else None

        if end > datetime.now():
            end = datetime.now()
        if start < datetime(year=2021, month=1, day=4):
            start = datetime(year=2021, month=1, day=4)         

        fetched_df = self._fetch_filtered_orderbook_data_threads(market, start, end, _tickers, _phases)
        fetched_df['transactionprice'] = fetched_df['transactionprice'].replace('NOAP', '0')
        fetched_df['transactionprice'] = fetched_df['transactionprice'].fillna('0')
        return fetched_df
    
    def get_max_date(self, market: MARKET) -> Optional[datetime]:
        files = os.listdir(self.root)
        dates = [f.split('_')[-1].split('.')[0] for f in files if f.startswith(f"ORK_Orders_{market}")]
        dates = [datetime.strptime(date, '%Y%m%d') for date in dates]
        if not dates:
            raise None
        return max(dates)
    
    def get_min_date(self, market: MARKET) -> Optional[datetime]:
        files = os.listdir(self.root)
        dates = [f.split('_')[-1].split('.')[0] for f in files if f.startswith(f"ORK_Orders_{market}")]
        dates = [datetime.strptime(date, '%Y%m%d') for date in dates]
        if not dates:
            raise None
        return min(dates)    
        
    def _fetch_filtered_orderbook_data_threads(
        self, 
        market: MARKET,
        start: datetime, 
        end: datetime,
        tickers: Optional[tuple[str]] = None,
        phases: Optional[tuple[PHASE]] = None
    ) -> pd.DataFrame:
        """Multithreaded no GIL IO"""
        date_idx = pd.date_range(start=start, end=end, freq="B")
        n_threads = min(6, date_idx.shape[0] // 30 + 1)
        chunksize = max(1, date_idx.shape[0] // n_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            res = executor.map(functools.partial(self._get_file_date, market=market, tickers=tickers, phases=phases), date_idx.to_pydatetime(), chunksize=chunksize)
            all_orders = [df for df in res if df is not None]
            if len(all_orders) < 1:
                raise FileNotFoundError(f"No data found for {market} from {start} to {end} for tickers {tickers} and phases {phases}")
            return pd.concat(all_orders, axis=0, ignore_index=True)
    
    def _get_file_date(
        self,
        current_time: datetime, 
        market: str,
        tickers: Optional[tuple[str]] = None,
        phases: Optional[tuple[PHASE]] = None
    ) -> pd.DataFrame | None:
        ORDERS_FNAME = f"ORK_Orders_{market}_INET_FSALT_"
        PHASES_NAME = f"ORK_Trading_Phases_{market}_INET_FSALT_"
        PRICES_NAME = f"ORK_Equilibrium_Prices_{market}_INET_FSALT_"
        orders_filename = ORDERS_FNAME + current_time.strftime('%Y%m%d') + ".csv.gz"
        phases_filename = PHASES_NAME + current_time.strftime('%Y%m%d') + ".csv.gz"
        prc_filename = PRICES_NAME + current_time.strftime('%Y%m%d') + ".csv.gz"
        dtype_map = defaultdict(lambda: str)
        dtype_map["seqnum"] = int
        try:
            # get orders
            fetched_orders = pd.read_csv(self.root+orders_filename, usecols=orderbook_cols, index_col=0, engine="c", compression="gzip", dtype=dtype_map)
            if tickers:
                fetched_orders = fetched_orders.loc[fetched_orders.orderbookcode.isin(tickers)]
            if fetched_orders.shape[0] < 1:
                logger.warning(f"No order data available for {orders_filename}, skipping this day.")
                return None
            fetched_orders = fetched_orders.sort_values('seqnum')
            # get phases
            fetched_phases = pd.read_csv(self.root+phases_filename, usecols=['seqnum', 'orderbookcode', 'tradingphases'], index_col=0, engine="c", compression="gzip", dtype=dtype_map)
            if fetched_phases.shape[0] < 1:
                # TODO: At the moment if no phase data available, skips the day
                logger.warning(f"No phase data available for {phases_filename}, skipping this day.")
                return None
            fetched_phases = fetched_phases.sort_values('seqnum')
            fetched_orders = pd.merge_asof(fetched_orders, fetched_phases, on='seqnum', by='orderbookcode')
            if phases:
                fetched_orders = fetched_orders.loc[fetched_orders.tradingphases.isin(phases)]
            if fetched_orders.shape[0] < 1:
                logger.warning(f"No order data available after phase filter for {orders_filename}, skipping this day.")
                return None
            # get prices
            fetched_prices = pd.read_csv(self.root+prc_filename, usecols=['seqnum', 'orderbookcode', 'indicativeauctionprice', 'indicativeauctionvolume'], index_col=0, engine="c", compression="gzip", dtype=dtype_map)
            if fetched_prices.shape[0] > 0:
                fetched_orders = pd.merge_asof(fetched_orders, fetched_prices, on='seqnum', by='orderbookcode')
            
            return fetched_orders
        except FileNotFoundError as e:
            return None
        except pd.errors.EmptyDataError as e:
            logger.error(f"Empty data encountered on file {orders_filename}. Error: {e}")
            return None
        except Exception as e:
            logger.exception(f"Unexpected error while processing date {str(current_time)}. Error: {e}")
            raise e
        