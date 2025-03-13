# -*- coding: utf-8 -*-
import pyodbc
import pandas as pd
import configparser


def fetch_data(stock_code, table_name):
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

    # 创建 SQL 查询语句
    sql_query = f"""
    SELECT * FROM {table_name}
    WHERE 股票代码 = ?
    ORDER BY 日期 DESC;
    """

    # 建立数据库连接
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # 执行 SQL 查询
    cursor.execute(sql_query, (stock_code,))

    # 获取查询结果并将其转换为 pandas DataFrame
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame.from_records(rows, columns=columns)

    # 获取前10行和后5行
    df_head = df.head(10)
    df_tail = df.tail(5)

    # 合并前10行和后5行
    df_combined = pd.concat([df_head, df_tail])

    # 格式化输出为所需的格式
    formatted_output = df_combined.to_string(index=False)
    print(formatted_output)

    # 关闭数据库连接
    cursor.close()
    conn.close()


def main(): 
    # 查询股票历史数据
    fetch_data('002848', 'dbo.StockPriceHistory')
    # 查询板块历史数据
    fetch_data('BK1041', 'dbo.SectorPriceHistory')


if __name__ == "__main__":
    main()
