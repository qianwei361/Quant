# -*- coding: utf-8 -*-
import pyodbc
import pandas as pd
import configparser


def fetch_stocks_without_data_on_date(exclude_date, table_name):
    # 从config.ini文件加载配置设置
    config = configparser.ConfigParser()
    config.read('config.ini')

    # 从配置文件中读取数据库连接参数
    server = config['Stocks']['server']
    database = config['Stocks']['database']
    username = config['Stocks']['username']
    password = config['Stocks']['password']
    driver = config['Stocks']['driver']

    # 创建连接字符串
    conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # 建立数据库连接
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # 查询所有股票代码
    cursor.execute(f"SELECT DISTINCT 股票代码 FROM {table_name}")
    all_stocks = cursor.fetchall()

    stocks_without_data = []

    # 对于每个股票代码，检查是否在指定日期有数据
    for stock in all_stocks:
        stock_code = stock[0]

        # 查询该股票在指定日期是否有数据
        cursor.execute(f"""
        SELECT 1 FROM {table_name}
        WHERE 股票代码 = ? AND 日期 = ?
        """, (stock_code, exclude_date))

        if cursor.fetchone() is None:
            # 如果没有数据，记录该股票代码
            stocks_without_data.append(stock_code)

    # 关闭数据库连接
    cursor.close()
    conn.close()

    # 返回没有数据的股票代码列表
    return stocks_without_data


def main():
    # 查询2025年2月10日没有数据的股票代码
    stocks_without_data = fetch_stocks_without_data_on_date('2025-02-19', 'dbo.StockPriceHistory')

    # 输出没有数据的股票代码
    print("以下股票在2025年2月19日没有数据：")
    for stock in stocks_without_data:
        print(stock)


if __name__ == "__main__":
    main()
