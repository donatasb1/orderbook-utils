from abc import ABC, abstractmethod
from typing import List, Optional, Union, Any
from datetime import datetime
from orderbook.MarketTypes import MARKET, PHASE
from pandas import DataFrame


orderbook_cols = ['submittingentityid', 'dea', 'clientidcode',
       'investmentdecisionwithinfirm', 'execwithinfirm', 'nonexecutingbroker',
       'tradingcapacity', 'liquidityprovisionactivity', 'dateandtime',
       'validityperiod', 'orderrestriction', 'validityperiodandtime',
       'prioritytimestamp', 'prioritysize', 'seqnum', 'mic', 'orderbookcode',
       'financialinstrumentidcode', 'dateofreceipt', 'orderidcode',
       'orderevent', 'ordertype', 'ordertypeclass', 'limitprice',
       'additionallimitprice', 'stopprice', 'peggedlimitprice',
       'transactionprice', 'pricecurrency', 'currencyleg2', 'pricenotation',
       'buysellind', 'orderstatus', 'quantitynotation', 'quantitycurrency',
       'initialqty', 'remainingqtyinclhidden', 'displayedqty',
       'tradedquantity', 'minacceptableqty', 'minimumexecutablesize',
       'mesfirstexeconly', 'passiveonly', 'passiveoraggressive',
       'selfexecutionprevention', 'strategylinkedorderid', 'routingstrategy',
       'tradingvenuetransactionidcode']


class OrderbookDB(ABC):
    """Orderbook database interface for fetching orderbook data"""
    @abstractmethod
    def fetch_filtered_orderbook_data(
        self, 
        market: MARKET,
        start: datetime, 
        end: datetime,
        tickers: Optional[Union[str, List[str]]],        
        phases: Optional[Union[PHASE, List[PHASE]]]        
    ) -> Optional[DataFrame]:
        pass

    @abstractmethod
    def get_max_date(self, market: MARKET) -> Optional[datetime]:
        pass

    @abstractmethod
    def get_min_date(self, market: MARKET) -> Optional[datetime]:
        pass


class OrderbookStatsDB(ABC):
    """Abstract base class for orderbook statistics database operations."""

    @abstractmethod
    def get_max_date(self) -> Optional[datetime]:
        """Returns the maximum date in the orderbook statistics database."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def get_dates_between(self, start: datetime, end: datetime) -> List[datetime]:
        """
        Returns distinct orderbook statistics dates between given dates.
        If no data is available, returns an empty list.
        """
        raise NotImplementedError("Subclasses must implement this method.")
    
    @abstractmethod
    def get_date_exists(self, date: datetime) -> bool:
        """
        Checks if a specific date exists in the orderbook statistics database.
        Returns True if the date exists, False otherwise.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def write_stats_df(self, df: DataFrame):
        """
        Writes a DataFrame of orderbook statistics to the database.
        If the DataFrame is empty, it should not raise an error.
        """
        raise NotImplementedError("Subclasses must implement this method.")