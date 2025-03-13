import akshare as ak
import pandas as pd
import talib
import json

def fetch_and_process_shanghai_index():
    try:
        # 获取上证指数历史数据
        df = ak.stock_zh_index_daily_em(symbol="sh000001")
        if df.empty:
            print("No data found for 上证指数.")
            return

        # 确保数据类型正确并计算所需指标
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        close_prices = df['close'].values
        volumes = df['volume'].values

        # 计算移动平均 MA 和成交量平均 VOL
        for timeperiod in [5, 10, 20, 30, 60]:
            df[f'MA{timeperiod}'] = talib.MA(close_prices, timeperiod).round(3)
            df[f'VOL{timeperiod}'] = talib.MA(volumes, timeperiod).round(3)

        # 重命名字段（在所有计算完成后进行）
        df.rename(columns={
            'date': '日期',
            'open': '开盘',
            'close': '收盘',
            'high': '最高',
            'low': '最低',
            'volume': '成交量',
            'amount': '成交额'
        }, inplace=True)

        # 删除不需要的列（可选，确保没有遗留的重复列）
        # 如果你不需要原始列，可以明确指定保留的列
        df = df[['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额',
                 'MA5', 'MA10', 'MA20', 'MA30', 'MA60',
                 'VOL5', 'VOL10', 'VOL20', 'VOL30', 'VOL60']]

        # 转换为字典格式
        data = df.to_dict(orient='records')

        # 保存为 JSON 文件
        with open('MarketData.json', 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

        print("上海证券指数历史数据和指标已保存为 'MarketData.json'.")
    except Exception as e:
        print(f"Error fetching or processing 上证指数 data: {e}")

def main():
    fetch_and_process_shanghai_index()

if __name__ == "__main__":
    main()
