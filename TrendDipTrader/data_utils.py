# data_utils.py
import json
import logging
import pandas as pd
import sqlalchemy
import os
import configparser
from datetime import datetime
from functools import lru_cache


config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"), encoding="utf-8")

log_level = config.get("logging", "level").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),  # 动态设置日志级别
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DATABASE_URL = config.get("database", "url")
engine = sqlalchemy.create_engine(DATABASE_URL)

STOCK_CODES_PATH = config.get("paths", "stock_codes")


@lru_cache(maxsize=1)
def load_all_stock_codes():
    with open(STOCK_CODES_PATH, 'r', encoding="utf-8") as file:
        return json.load(file)


def get_all_stocks():
    return load_all_stock_codes()


def load_all_data_60days():
    try:
        with engine.connect() as conn:
            sql = """
            SELECT 股票代码, 日期, 开盘, 收盘, 最高, 最低, 涨跌幅, 振幅, 换手率, MA5, MA20, MA60, VOL5
            FROM StockPriceHistory
            WHERE 日期 >= DATEADD(DAY, -60, GETDATE())
            ORDER BY 日期 DESC
            """
            df = pd.read_sql(sql, conn)

        cols_to_convert = ['开盘', '收盘', '最高', '最低', '涨跌幅', '振幅', '换手率', 'MA5', 'MA20', 'MA60', 'VOL5']
        for col in cols_to_convert:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    except sqlalchemy.exc.SQLAlchemyError as e:
        logging.error(f"数据库查询失败: {e}")
    except Exception as e:
        logging.error(f"发生未知错误: {e}")

    return pd.DataFrame()


def safe_float_conversion(value):
    try:
        return float(value)
    except ValueError:
        if value == '-':
            return None
        return None


def is_within_trading_hours():
    now = datetime.now()
    if now.weekday() < 7:
        morning_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        morning_end = now.replace(hour=13, minute=30, second=0, microsecond=0)
        afternoon_start = now.replace(hour=13, minute=30, second=0, microsecond=0)
        afternoon_end = now.replace(hour=23, minute=59, second=0, microsecond=0)
        return (morning_start <= now <= morning_end) or (afternoon_start <= now <= afternoon_end)
    return False
