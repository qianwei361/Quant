# market_trend.py
import logging
from data_utils import safe_float_conversion


def calculate_market_trend(all_realtime_data):
    """
    计算市场趋势：上涨股票的比例（%）

    参数:
        all_realtime_data: 实时股票数据 DataFrame

    返回:
        float: 上涨股票的比例（%）
    """
    try:
        price_changes = all_realtime_data['price_change_percent'].apply(safe_float_conversion)
        if price_changes.empty:
            logging.warning("没有有效的涨跌幅数据，返回默认市场趋势 0")
            return 0
        positive_count = (price_changes > 0).sum()
        total_count = len(price_changes)
        return (positive_count / total_count) * 100
    except KeyError as e:
        logging.error(f"数据中缺少 'price_change_percent' 列: {e}")
        return 0
    except Exception as e:
        logging.error(f"计算市场趋势时发生错误: {e}")
        return 0