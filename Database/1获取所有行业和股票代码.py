import efinance as ef
import json


def get_all_stock_codes():
    """获取所有股票的实时行情数据并返回股票代码列表"""
    stock_data = ef.stock.get_realtime_quotes()
    return stock_data['股票代码'].tolist()


def get_sector_stock_codes():
    """获取行业板块的实时行情数据并返回股票代码列表"""
    stock_data = ef.stock.get_realtime_quotes('行业板块')
    return stock_data['股票代码'].tolist()


def save_to_json(data, filename):
    """将数据保存为 JSON 文件"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"股票代码已保存到 '{filename}'")


def main():
    # 获取所有股票代码并保存
    all_stock_codes = get_all_stock_codes()
    save_to_json(all_stock_codes, 'C:\QuantTrader\Database\stock_codes.json')

    # 获取行业板块股票代码并保存
    sector_stock_codes = get_sector_stock_codes()
    save_to_json(sector_stock_codes, 'C:\QuantTrader\Database\sector_codes.json')


if __name__ == "__main__":
    main()