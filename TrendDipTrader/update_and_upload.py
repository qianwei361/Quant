# update_and_upload.py
# -*- coding: utf-8 -*-
import json
import efinance as ef
import pandas as pd
from sqlalchemy import create_engine
from configparser import ConfigParser
from concurrent.futures import ProcessPoolExecutor
import numpy as np
import os


def read_config(config_path='config.ini'):
    config = ConfigParser()
    with open(config_path, 'r', encoding='utf-8') as configfile:
        config.read_file(configfile)
    return config


def create_database_engine(database_url):
    return create_engine(database_url)


def get_all_stock_codes():
    stock_data = ef.stock.get_realtime_quotes()
    stock_codes = stock_data['股票代码'].tolist()
    return stock_codes


def save_stock_codes_to_json(stock_codes, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(stock_codes, f, ensure_ascii=False, indent=4)


def update_stock_codes(config):
    stock_codes_path = config.get('paths', 'stock_codes')
    stock_codes = get_all_stock_codes()
    save_stock_codes_to_json(stock_codes, stock_codes_path)
    print(f"股票代码已更新并保存到 {stock_codes_path}")


def fetch_and_compute(stock_code, database_url, table_name):
    try:
        df = ef.stock.get_quote_history(stock_code)
        if df.empty:
            print(f"股票代码 {stock_code} 无数据")
            return None

        latest_data = df.iloc[-2].copy()

        for ma in [5, 10, 20, 30, 60]:
            column_name = f'MA{ma}'
            vol_column_name = f'VOL{ma}'

            if len(df) >= ma:
                latest_data[column_name] = round(df['收盘'].rolling(window=ma).mean().iloc[-2], 2)
                latest_data[vol_column_name] = round(df['成交量'].rolling(window=ma).mean().iloc[-2], 2)
            else:
                latest_data[column_name] = np.nan
                latest_data[vol_column_name] = np.nan

        latest_data['成交量'] = latest_data['成交量']
        engine = create_engine(database_url)

        with engine.connect() as conn:
            latest_data_df = pd.DataFrame([latest_data])
            latest_data_df = latest_data_df.astype({
                'MA5': 'float', 'MA10': 'float', 'MA20': 'float', 'MA30': 'float', 'MA60': 'float',
                'VOL5': 'float', 'VOL10': 'float', 'VOL20': 'float', 'VOL30': 'float', 'VOL60': 'float'
            })
            latest_data_df.to_sql(table_name, con=conn, if_exists='append', index=False)

        return latest_data

    except pd.errors.EmptyDataError:
        print(f"股票代码 {stock_code} 无数据")
        return None
    except Exception as e:
        print(f"处理股票代码 {stock_code} 时发生错误")
        return None


def fetch_and_compute_wrapper(args):
    return fetch_and_compute(*args)


def main():
    config = read_config()

    update_stock_codes(config)

    database_url = config.get('database', 'url')

    stock_codes_path = config.get('paths', 'stock_codes')
    sector_codes_path = config.get('paths', 'sector_codes')

    stock_info = [
        {"path": stock_codes_path, "table": "StockPriceHistory"},
        {"path": sector_codes_path, "table": "SectorPriceHistory"}
    ]

    for info in stock_info:
        if not os.path.exists(info["path"]):  # 避免错误
            print(f"错误: {info['path']} 文件不存在，跳过处理")
            continue

        with open(info["path"], 'r', encoding='utf-8') as file:
            stock_codes = json.load(file)

        with ProcessPoolExecutor() as executor:
            list(executor.map(fetch_and_compute_wrapper, [(code, database_url, info["table"]) for code in stock_codes]))

    print("数据处理完成，已上传到数据库。")


if __name__ == "__main__":
    main()
