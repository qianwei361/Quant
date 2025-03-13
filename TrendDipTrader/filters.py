# filters.py
import logging
from data_utils import safe_float_conversion
import configparser


def filter_candidates(df_all):
    if df_all.empty:
        logging.warning("从数据库加载的数据为空，无法筛选任何股票。")
        return []

    grouped = df_all.groupby('股票代码', group_keys=False)
    qualified_list = []

    for stock_code, group_data in grouped:
        history_60 = group_data.head(60)
        if history_60.empty:
            continue

        if stock_code.startswith('688') or stock_code.startswith('8')or stock_code.startswith('4') or'ST' in stock_code:
            continue

        # 检查过去60天是否有涨停（日涨幅 >9.9% 且收盘价等于最高价）
        limit_up_count = ((history_60['涨跌幅'] > 9.9) & (history_60['最高'] == history_60['收盘'])).sum()
        if limit_up_count < 1:
            continue

        # 获取最近10天历史数据（仍然是降序）
        history_10 = history_60.head(10)
        condition1 = history_10.iloc[0]['MA5'] > history_10.iloc[0]['MA20']
        max_consecutive_growth = 0
        current_consecutive_growth = 0
        for i in range(len(history_10) - 1):  # 从旧到新遍历
            if history_10.iloc[i]['MA20'] < history_10.iloc[i + 1]['MA20']:
                current_consecutive_growth += 1
                max_consecutive_growth = max(max_consecutive_growth, current_consecutive_growth)
            else:
                current_consecutive_growth = 0
        condition2 = max_consecutive_growth >= 3
        if not (condition1 or condition2):
            continue

        # 识别 MA5 谷底（局部最低点）
        ma5_valley_index = None
        for i in range(1, len(history_10) - 1):
            if history_10.iloc[i+1]['MA5'] > history_10.iloc[i]['MA5'] < history_10.iloc[i-1]['MA5']:
                decrease_count = 0
                for j in range(i+1, len(history_10)):
                    if history_10.iloc[j]['MA5'] > history_10.iloc[j-1]['MA5']:
                        decrease_count += 1
                    else:
                        break
                if decrease_count > 2:
                    ma5_valley_index = i
                    break
        if ma5_valley_index is None:
            continue

        # 谷底之后，MA5 至少连续上涨2天
        ma5_growth_count = 0
        for k in range(ma5_valley_index - 1, -1, -1):
            if history_10.iloc[k]['MA5'] > history_10.iloc[k+1]['MA5']:
                ma5_growth_count += 1
            else:
                break
        if ma5_growth_count > 5:  # 连续上涨天数超过5天则跳过
            continue

        # 提取所需数据并添加到结果列表
        past_4_close_sum = history_10['收盘'].head(4).sum()      # 最近4天收盘价之和
        previous_low_price = history_10.iloc[0]['最低']           # 最新一天（上一交易日）的最低价
        previous_ma5 = history_10.iloc[0]['MA5']                 # 最新一天的 MA5
        qualified_list.append({
            '股票代码': stock_code,
            '过去4天收盘价之和': past_4_close_sum,
            '前一天最低价': previous_low_price,
            '前一天MA5': previous_ma5
        })

    return qualified_list



def filter_stocks(realtime_data, stocks_info, existing_stocks):
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')
    field_mappings = dict(config['field_mappings'])
    realtime_data = realtime_data.rename(columns=field_mappings)
    stocks_info_dict = {info['股票代码']: info for info in stocks_info if info}
    new_stocks = []
    for stock in realtime_data.itertuples():
        stock_code = getattr(stock, 'code')  # 使用映射后的英文字段
        if stock_code in stocks_info_dict and stock_code not in existing_stocks:
            latest_price = safe_float_conversion(getattr(stock, 'latest_price'))
            price_change = safe_float_conversion(getattr(stock, 'price_change_percent'))
            open_price = safe_float_conversion(getattr(stock, 'open'))
            high_price = safe_float_conversion(getattr(stock, 'high'))
            low_price = safe_float_conversion(getattr(stock, 'low'))
            prev_close_price = safe_float_conversion(getattr(stock, 'previous_close'))
            total_market_value = safe_float_conversion(getattr(stock, 'circulating_market_value'))
            past_4_close_sum = stocks_info_dict[stock_code].get('过去4天收盘价之和')
            previous_low_price = stocks_info_dict[stock_code].get('前一天最低价')
            previous_ma5 = stocks_info_dict[stock_code].get('前一天MA5')
            if None in [latest_price, price_change, open_price, high_price, low_price, prev_close_price, total_market_value, past_4_close_sum, previous_low_price, previous_ma5]:
                logging.warning(f"{stock_code} 关键数据缺失，跳过筛选")
                continue
            realtime_ma5 = (past_4_close_sum + latest_price) / 5

            if (
                    latest_price > 2 and #最新价大于2元
                    realtime_ma5 > previous_ma5 and  #今天ma5大于前一天ma5
                    realtime_ma5 * 0.98 < latest_price < realtime_ma5 * 1.02 and  #最新价大于ma5小于ma5*1.02
                    (previous_low_price < previous_ma5 or low_price < realtime_ma5) and  #前后2天股价至少有一天低于其当天ma5
                    20e8 <= total_market_value <= 500e8  #20亿到500亿
            ):
                new_stocks.append((stock_code, latest_price))
                existing_stocks.add(stock_code)

    return new_stocks