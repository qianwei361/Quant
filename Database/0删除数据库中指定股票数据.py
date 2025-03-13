import sqlalchemy
from sqlalchemy import create_engine, text
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 创建数据库引擎
def create_db_engine():
    database_url =  "mssql+pyodbc://sa:123456@localhost/Quant?driver=ODBC+Driver+17+for+SQL+Server"
    return create_engine(database_url)

# 删除指定股票代码的所有数据
def delete_all_data_by_codes(stock_codes):
    try:
        engine = create_db_engine()
        with engine.connect() as conn:
            for code in stock_codes:
                query = text("""
                DELETE FROM StockPriceHistory
                WHERE 股票代码 = :code
                """)
                conn.execute(query, {"code": code})
                conn.commit()  # 提交事务
                logging.info(f"删除了股票代码的所有数据: {code}")
    except Exception as e:
        logging.error(f"删除数据失败: {e}")

if __name__ == "__main__":
    stock_codes = ['000040']
    delete_all_data_by_codes(stock_codes)
#
#
# import sqlalchemy
# from sqlalchemy import create_engine, text
# import logging
#
# # 配置日志
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#
# # 创建数据库引擎
# def create_db_engine():
#     database_url =  "mssql+pyodbc://sa:123456@localhost/Quant?driver=ODBC+Driver+17+for+SQL+Server"
#     return create_engine(database_url)
#
# # 删除所有股票代码以BK开头的数据
# def delete_all_data_by_BK_codes():
#     try:
#         engine = create_db_engine()
#         with engine.connect() as conn:
#             # 注意：这里使用了LIKE匹配，删除所有以BK开头的记录
#             query = text("""
#                 DELETE FROM StockPriceHistory
#                 WHERE 股票代码 LIKE 'BK%'
#             """)
#             conn.execute(query)
#             conn.commit()  # 提交事务
#             logging.info("删除了所有BK开头的股票代码数据")
#     except Exception as e:
#         logging.error(f"删除数据失败: {e}")
#
# if __name__ == "__main__":
#     # 调用示例
#     delete_all_data_by_BK_codes()
#
#
#
# import sqlalchemy
# from sqlalchemy import create_engine, text
# import logging
#
# # 配置日志
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#
# # 创建数据库引擎
# def create_db_engine():
#     database_url = "mssql+pyodbc://sa:123456@localhost/Quant?driver=ODBC+Driver+17+for+SQL+Server"
#     return create_engine(database_url)
#
# # 删除 StockPriceHistory 表中指定日期的数据
# def delete_data_by_date(target_date):
#     """
#     删除 StockPriceHistory 表中指定日期的数据
#     :param target_date: 目标日期 (字符串格式 'YYYY-MM-DD')
#     """
#     try:
#         engine = create_db_engine()
#         with engine.connect() as conn:
#             with conn.begin():
#                 query = text("DELETE FROM StockPriceHistory WHERE 日期 = :target_date")
#                 conn.execute(query, {"target_date": target_date})
#                 logging.info(f"成功删除 StockPriceHistory 表中 {target_date} 的数据。")
#     except Exception as e:
#         logging.error(f"删除数据失败: {e}")
#
# if __name__ == "__main__":
#     target_date = "2025-02-12"  # 替换成你要删除的日期
#     delete_data_by_date(target_date)

