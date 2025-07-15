import logging
from orderbook.utils import setup_logs
setup_logs()
from datetime import datetime, timedelta
import pandas as pd
from orderbook.db.csvreader import OrderbookCSVA
from orderbook.db.statssqlite import OrderbookStatsSqlite
import orderbook.etl.orderbookstats as orderbookstats
from orderbook.MarketTypes import MARKET

logger = logging.getLogger(__name__)

source_db = OrderbookCSVA(path="data_20250616/")
stats_db = OrderbookStatsSqlite(db_path='orderbook_stats.db')

def process_new_files(market: MARKET):
    """Process new files for the given market and update the stats database."""
    max_filedate = source_db.get_max_date(market)
    last_stats_date = stats_db.get_max_date()
    if last_stats_date is None:
        last_stats_date = datetime(2025, 3, 17)
    next_stats_date = last_stats_date + timedelta(days=1)
    process_date_range(market, next_stats_date, max_filedate)


def process_date_range(market: MARKET, start_date: datetime, end_date: datetime):
    next_stats_date = start_date
    logger.info(f"Processing date range from {start_date} to {end_date} for market: {market}")    
    while next_stats_date <= end_date:
        try:
            process_date(market, next_stats_date)
        except Exception as e:
            logger.exception(f"Failed to update stats for {market} on {next_stats_date}: {e}")
        finally:
            next_stats_date += timedelta(days=1)


def extract_date(market: MARKET, date: datetime) -> pd.DataFrame:
    try:
        data = source_db.fetch_filtered_orderbook_data(
            start=date,
            end=date,
            market=market,
        )
        if data.empty:
            logger.warning(f"No data found for {market} on {date}")
            return
        return data
    except FileNotFoundError:
        logger.warning(f"No data found for {market} on {date}")
        return

def transform_data(market: MARKET, data: pd.DataFrame) -> pd.DataFrame:
    try:
        daily_stats = orderbookstats.get_daily_stats(data)
        if daily_stats.empty:
            logger.warning(f"No daily stats computed for {market} on {data['dateandtime'].iloc[0]}")
            return pd.DataFrame()
    except Exception as e:
        logger.exception(f"Failed to compute daily stats for {market} on {data['dateandtime'].iloc[0]}: {e}")
        return pd.DataFrame()

    daily_stats['date'] = pd.to_datetime(data['dateandtime'].iloc[0]).date()
    daily_stats['market'] = market
    return daily_stats

def load_data(market: MARKET, daily_stats: pd.DataFrame):
    try:
        stats_db.write_stats_df(daily_stats)
        logger.info(f"Processed and inserted stats for {market} on {daily_stats['date'].iloc[0]}")
    except Exception as e:
        logger.exception(f"Failed to insert stats for {market} on {daily_stats['date'].iloc[0]}: {e}")


def process_date(market: MARKET, date: datetime):
    """Process a specific date for the given market."""
    logger.info(f"Processing date {date} for market {market}")
    assert date >= datetime(2025, 3, 17), "Date must be after the start of the dataset"
    if stats_db.get_date_exists(date):
        logger.info(f"Stats for {market} on {date} already exist, skipping.")
        return
    logger.info(f"Extracting data for {market} on {date}")
    data = extract_date(market, date)
    if data is None:
        return
    logger.info(f"Transforming data for {market} on {date}")
    daily_stats = transform_data(market, data)
    if daily_stats.empty:
        return
    load_data(market, daily_stats)

