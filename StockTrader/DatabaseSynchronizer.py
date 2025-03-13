import pandas as pd
import pyodbc
import sqlalchemy
from pathlib import Path
import json
import configparser
import os

# 从 config.ini 读取数据库连接参数和文件路径
config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')

# 从配置文件获取数据库连接字符串
db_url = config.get('database', 'url')

# 获取股票和行业板块数据文件夹路径
script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
stock_folder_path = script_dir / "stock_history_data"
sector_folder_path = script_dir / "sector_history_data"

# 定义预期的数据列
expected_columns = [
    '股票名称', '股票代码', '日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅',
    '涨跌幅', '涨跌额', '换手率', 'MA5', 'MA10', 'MA20', 'MA30', 'MA60',
    'VOL5', 'VOL10', 'VOL20', 'VOL30', 'VOL60'
]

# 字段映射（从中文列名到数据库列名）
field_mappings = {key: value for key, value in config.items('field_mappings') if value}

# 数据库插入查询语句
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
def process_files(folder_path, insert_query, data_type):
    print(f"开始处理{data_type}数据...")

    # 检查文件夹是否存在
    if not folder_path.exists():
        print(f"文件夹不存在: {folder_path}")
        return

    stock_files = list(folder_path.glob('*.json'))
    print(f"找到 {len(stock_files)} 个 JSON 文件")

    # 创建数据库连接
    try:
        # 使用 SQLAlchemy 连接数据库
        engine = sqlalchemy.create_engine(db_url)
        connection = engine.connect()
        print("成功连接到数据库")
    except Exception as e:
        print(f"数据库连接错误: {e}")
        return

    processed_count = 0
    error_count = 0

    for file_path in stock_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                stock_data = json.load(file)

            if not stock_data:
                print(f"文件为空: {file_path}")
                continue

            # 将数据转换成 DataFrame
            df = pd.DataFrame(stock_data)

            # 处理缺失列
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = None

            # 确保列顺序一致
            df = df[expected_columns]

            # 处理 NaN 值
            df = df.fillna(0)

            # 使用 SQLAlchemy 的批量插入功能
            table_name = "StockPriceHistory" if data_type == "股票" else "SectorPriceHistory"
            df.to_sql(table_name, engine, if_exists='append', index=False)

            processed_count += 1
            print(f"成功处理文件: {file_path.name}")

        except Exception as e:
            error_count += 1
            print(f"处理文件 {file_path} 时出错: {e}")

    # 关闭连接
    connection.close()

    print(f"{data_type}数据处理完成。成功: {processed_count}, 错误: {error_count}")


# 运行程序
def main():
    print("开始执行数据库导入程序...")
    process_files(stock_folder_path, queries['stock'], "股票")
    process_files(sector_folder_path, queries['sector'], "行业板块")
    print("程序执行完毕")


if __name__ == '__main__':
    main()