import sqlite3
import pandas as pd
from orderbook.db.base import OrderbookStatsDB
from datetime import datetime
from typing import List, Optional


class SqliteContext:
    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = sqlite3.connect(self._db_path)

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc_value, traceback):
        if self._conn:
            self._conn.close()


class OrderbookStatsSqlite(OrderbookStatsDB):

    def __init__(self, db_path: str = 'orderbook_stats.db'):
        self._db_path = db_path
        self._init_table()

    @property
    def db_context(self):
        """Returns a context manager for the database connection."""
        return SqliteContext(self._db_path)

    def _init_table(self):
        with self.db_context as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orderbook_stats (
                    date DATE NOT NULL,
                    market TEXT NOT NULL,
                    orderbookcode TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    trade_count INTEGER,
                    top_spread REAL,
                    buy_5_depth REAL,
                    sell_5_depth REAL,
                    imbalance REAL,
                    PRIMARY KEY (date, market, orderbookcode)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_orderbook_stats_date ON orderbook_stats (date);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_orderbook_stats_orderbookcode ON orderbook_stats (orderbookcode);
            """)
            conn.commit()
            cur.close()
    
    def get_max_date(self) -> datetime:
        with self.db_context as conn:
            cur = conn.cursor()
            cur.execute("SELECT MAX(date) FROM orderbook_stats")
            max_date = cur.fetchone()[0]
            cur.close()
            if max_date is None:
                return None
            return datetime.strptime(max_date, '%Y-%m-%d')

    def get_dates_between(self, start: datetime, end: datetime) -> List[datetime]:
        with self.db_context as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT date FROM orderbook_stats
                WHERE date BETWEEN ? AND ?
                ORDER BY date
            """, (start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')))
            dates = cur.fetchall()
            cur.close()
            return [datetime.strptime(date[0], '%Y-%m-%d') for date in dates]
        
    def get_date_exists(self, date: datetime) -> bool:
        with self.db_context as conn:
            cur = conn.cursor()
            cur.execute("SELECT EXISTS(SELECT 1 FROM orderbook_stats WHERE date = ?)", (date.strftime('%Y-%m-%d'),))
            exists = cur.fetchone()[0]
            cur.close()
            return bool(exists)

    def write_stats_df(self, df: pd.DataFrame):
        """Write a DataFrame of orderbook stats to the database."""
        with self.db_context as conn:
            df.to_sql('orderbook_stats', conn, if_exists='append', index=False)
            conn.commit()
