import pandas as pd
import pyodbc
from pathlib import Path
import json
import configparser

# 从 config.ini 读取数据库连接参数和文件路径
config = configparser.ConfigParser()
config.read('config.ini')

server = config.get('Stocks', 'server')
database = config.get('Stocks', 'database')
username = config.get('Stocks', 'username')
password = config.get('Stocks', 'password')
driver = config.get('Stocks', 'driver')
stock_folder_path = Path(config.get('Paths', 'stock_data_folder'))
sector_folder_path = Path(config.get('Paths', 'sector_data_folder'))

connection_string = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

expected_columns = [
    '股票名称', '股票代码', '日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅',
    '涨跌幅', '涨跌额', '换手率', 'MA5', 'MA10', 'MA20', 'MA30', 'MA60',
    'VOL5', 'VOL10', 'VOL20', 'VOL30', 'VOL60'
]

queries = {
    'stock': f'''
    INSERT INTO StockPriceHistory ({', '.join(expected_columns)})
    VALUES ({', '.join(['?' for _ in expected_columns])})
    ''',
    'sector': f'''
    INSERT INTO SectorPriceHistory ({', '.join(expected_columns)})
    VALUES ({', '.join(['?' for _ in expected_columns])})
    '''
}

# 处理 JSON 文件并插入数据库
def process_files(folder_path, insert_query):
    stock_files = list(folder_path.glob('*.json'))
    for file_path in stock_files:
        with open(file_path, 'r', encoding='utf-8') as file:
            stock_data = json.load(file)
        if not stock_data:
            continue

        # 将数据转换成 DataFrame，并确保列顺序一致
        df = pd.DataFrame(stock_data)

        # 处理缺失列，确保 DataFrame 具有所有必需列
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None  # 如果 JSON 缺少某列，则填充 None

        # 确保列顺序一致
        df = df[expected_columns]

        # 处理 NaN 值，避免数据库报错
        df = df.fillna(0)

        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            for _, row in df.iterrows():
                try:
                    cursor.execute(insert_query, tuple(row))
                    conn.commit()
                except pyodbc.Error as e:
                    print("数据库插入错误:", e)
                    print("错误的行:", row.to_dict())
                    conn.rollback()

# 运行程序
def main():
    process_files(stock_folder_path, queries['stock'])
    process_files(sector_folder_path, queries['sector'])

if __name__ == '__main__':
    main()
