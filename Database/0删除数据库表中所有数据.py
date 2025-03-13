import sqlalchemy
from sqlalchemy import create_engine, text
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 创建数据库引擎
def create_db_engine():
    database_url = "mssql+pyodbc://sa:123456@localhost/Quant?driver=ODBC+Driver+17+for+SQL+Server"
    return create_engine(database_url)

# 删除 StockPriceHistory 表中的所有数据
def delete_all_data_from_table():
    try:
        engine = create_db_engine()
        with engine.connect() as conn:
            # 开始一个事务
            with conn.begin():
                query = text("DELETE FROM StockPriceHistory")
                conn.execute(query)
                logging.info("成功删除 StockPriceHistory 表中的所有数据。")
    except Exception as e:
        logging.error(f"删除数据失败: {e}")

if __name__ == "__main__":
    delete_all_data_from_table()
