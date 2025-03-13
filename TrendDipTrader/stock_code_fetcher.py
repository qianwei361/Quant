# stock_code_fetcher.py
import efinance as ef
import json
import logging
import configparser  # 新增：用于读取 config.ini

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_and_save_stock_codes(config_path='config.ini'):
    try:
        # 读取 config.ini 文件
        config = configparser.ConfigParser()
        config.read(config_path, encoding='utf-8')

        # 获取保存路径
        output_path = config.get('paths', 'stock_codes')

        # 获取股票代码列表
        stock_data = ef.stock.get_realtime_quotes()
        stock_codes = stock_data['股票代码'].tolist()

        # 保存到 JSON 文件，设置 indent=4 使列表竖排
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(stock_codes, f, ensure_ascii=False, indent=4)
        logging.info(f"股票代码列表已保存到 {output_path}")
    except Exception as e:
        logging.error(f"获取或保存股票代码列表失败: {e}")
