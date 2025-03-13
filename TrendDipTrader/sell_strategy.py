# sell_strategy.py
import logging
from data_utils import safe_float_conversion

def get_sell_candidates(all_realtime_data, account_info):
    """
    计算符合卖出条件的股票列表，返回账户ID + 股票代码
    :param all_realtime_data: 实时行情数据
    :param account_info: 当前所有账户持仓信息
    :return: 需要卖出的股票列表 [(账户ID, 股票代码)]
    """
    sell_candidates = []

    for account_id, holdings in account_info.items():  # 遍历所有账户
        for holding in (holdings or []):
            stock_code = holding.get('证券代码')
            available_balance = holding.get('可用余额', 0)
            cost_price = holding.get('成本价', 0)
            latest_price = None

            # 从行情数据中找到该股票的最新价格
            for stock in all_realtime_data.itertuples():
                if stock_code == getattr(stock, '代码'):
                    latest_price = safe_float_conversion(getattr(stock, '最新价'))
                    break

            # **检查数据有效性**
            if latest_price is None:
                logging.warning(f"无法获取 {stock_code} 的最新价，跳过该股票的卖出计算")
                continue

            if available_balance <= 0:
                continue

            if cost_price <= 0:
                logging.warning(f"{stock_code} 成本价异常: {cost_price}，无法计算盈亏比")
                continue

            # **计算盈亏比**
            profit_loss_ratio = (latest_price - cost_price) / cost_price * 100

            # **防止极端数值影响**
            if not (-100 < profit_loss_ratio < 100):
                logging.warning(f"{stock_code} 计算出的盈亏比异常: {profit_loss_ratio:.2f}%")
                continue

            # **盈亏比 > 5% 或 < -2% 时卖出**
            if profit_loss_ratio > 5 or profit_loss_ratio < -2:
                logging.info(f"盈亏比满足条件，卖出股票 {stock_code}, 当前盈亏比: {profit_loss_ratio:.2f}%")
                sell_candidates.append((account_id, stock_code))  # 记录账户ID和股票代码

    return sell_candidates
