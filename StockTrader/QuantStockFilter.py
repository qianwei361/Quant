import efinance as ef
import pandas as pd
import talib
import json
import os
import configparser
from multiprocessing import Pool
from datetime import datetime, timedelta


def get_all_stock_codes():
    """获取所有股票的实时行情数据并返回股票代码列表，剔除特定股票"""
    stock_data = ef.stock.get_realtime_quotes()
    filtered_stocks = []

    for index, row in stock_data.iterrows():
        stock_code = row['股票代码']
        stock_name = row['股票名称']

        # 剔除68开头，8开头，9开头，4开头的股票，以及名称包含ST的股票
        if (stock_code.startswith('68') or
                stock_code.startswith('8') or
                stock_code.startswith('9') or
                stock_code.startswith('4') or
                'ST' in stock_name):
            continue

        filtered_stocks.append(stock_code)

    print(f"总股票数: {len(stock_data)}, 过滤后剩余: {len(filtered_stocks)}")
    return filtered_stocks


def get_sector_stock_codes():
    """获取行业板块的实时行情数据并返回股票代码列表"""
    # 行业板块数据不需要进行股票代码过滤，因为它们不是个股代码
    stock_data = ef.stock.get_realtime_quotes('行业板块')
    return stock_data['股票代码'].tolist()


def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()
    config_path = 'config.ini'

    # 检查配置文件是否存在，如果不存在则提示错误
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件 '{config_path}' 不存在，请先创建该文件")

    # 读取配置文件，明确指定UTF-8编码
    config.read(config_path, encoding='utf-8')

    # 验证必要的配置项
    if 'paths' not in config or 'stock_codes' not in config['paths'] or 'sector_codes' not in config['paths']:
        raise ValueError("配置文件缺少必要的路径设置，请检查配置文件")

    config.read(config_path)
    return config


def save_to_json(data, filename):
    """将数据保存为 JSON 文件"""
    # 确保目录存在
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"数据已保存到 '{filename}'")


def get_start_date():
    """获取3个月前的日期，格式为'YYYYMMDD'"""
    today = datetime.now()
    three_months_ago = today - timedelta(days=90)
    return three_months_ago.strftime('%Y%m%d')


def fetch_and_process_history(data_type):
    """获取并处理历史数据"""
    # 创建输出目录
    output_dir = f"{data_type}_history_data"
    os.makedirs(output_dir, exist_ok=True)

    # 加载配置
    config = load_config()

    # 获取股票代码列表
    if data_type == 'stock':
        stock_codes = get_all_stock_codes()
        # 保存所有股票代码
        save_to_json(stock_codes, config['paths']['stock_codes'])
    elif data_type == 'sector':
        stock_codes = get_sector_stock_codes()
        # 保存行业板块代码
        save_to_json(stock_codes, config['paths']['sector_codes'])
    else:
        print("无效的数据类型。")
        return

    # 获取起始日期
    start_date = get_start_date()

    # 获取并保存历史数据
    for stock_code in stock_codes:
        try:
            history_data = ef.stock.get_quote_history(stock_code, beg=start_date, end='20500101')
            if history_data.empty:
                print(f"{data_type}代码 {stock_code} 没有数据")
                continue

            # 计算移动平均线
            history_data = process_history_data(history_data)

            # 保存处理后的数据
            history_json = history_data.to_json(orient='records', force_ascii=False, indent=4)
            file_path = os.path.join(output_dir, f'{stock_code}.json')
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(history_json)
            print(f"{data_type}代码 {stock_code} 的数据已保存。")
        except Exception as e:
            print(f"获取{data_type}代码 {stock_code} 的数据时出错: {e}")


def process_history_data(df):
    """处理历史数据，计算移动平均线"""
    if df.empty:
        return df

    df.sort_values('日期', inplace=True)
    df['收盘'] = df['收盘'].astype(float)
    df['成交量'] = df['成交量'].astype(float)

    close_prices = df['收盘'].values
    volumes = df['成交量'].values

    # 计算各种时间周期的移动平均线
    for timeperiod in [5, 10, 20, 30, 60]:
        df[f'MA{timeperiod}'] = talib.MA(close_prices, timeperiod).round(3)
        df[f'VOL{timeperiod}'] = talib.MA(volumes, timeperiod).round(3)

    return df


def main():
    # 处理股票数据
    fetch_and_process_history('stock')

    # 处理行业板块数据
    fetch_and_process_history('sector')


if __name__ == "__main__":
    main()