import efinance as ef
import pandas as pd
import talib
from multiprocessing import Pool
import os
import json


def fetch_and_save_history(data_type, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    if data_type == 'stock':
        with open('../TrendDipTrader/stock_codes.json', 'r', encoding='utf-8') as file:
            stock_codes = json.load(file)
    elif data_type == 'sector':
        industry_quotes = ef.stock.get_realtime_quotes('行业板块')
        stock_codes = industry_quotes['股票代码']
    else:
        print("Invalid data type specified.")
        return

    for stock_code in stock_codes:
        try:
            history_data = ef.stock.get_quote_history(stock_code, beg='20240101', end='20500101')
            if history_data.empty:
                print(f"No data for {data_type} code {stock_code}")
                continue
            history_json = history_data.to_json(orient='records', force_ascii=False, indent=4)
            file_path = os.path.join(output_dir, f'{stock_code}.json')
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(history_json)
            print(f"Data for {data_type} code {stock_code} saved.")
        except Exception as e:
            print(f"Error fetching data for {data_type} code {stock_code}: {e}")


def update_json_with_ma(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        df = pd.DataFrame(data)
        if df.empty:
            print(f"No data in file {file_path}")
            return
        df.sort_values('日期', inplace=True)
        df['收盘'] = df['收盘'].astype(float)
        df['成交量'] = df['成交量'].astype(float)
        close_prices = df['收盘'].values
        volumes = df['成交量'].values
        for timeperiod in [5, 10, 20, 30, 60]:
            df[f'MA{timeperiod}'] = talib.MA(close_prices, timeperiod).round(3)
            df[f'VOL{timeperiod}'] = talib.MA(volumes, timeperiod).round(3)
        updated_data = df.to_dict(orient='records')
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(updated_data, file, ensure_ascii=False, indent=4)
        print(f"File {file_path} updated.")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")


def main():
    fetch_and_save_history('stock', 'stock_history_data')
    fetch_and_save_history('sector', 'sector_history_data')
    for output_dir in ['stock_history_data', 'sector_history_data']:
        json_files = [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith('.json')]
        with Pool(os.cpu_count()) as pool:
            pool.map(update_json_with_ma, json_files)


if __name__ == "__main__":
    main()
