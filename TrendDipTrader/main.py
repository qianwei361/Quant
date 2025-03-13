# main.py
import logging
import time
import efinance as ef
import threading
import schedule
import configparser
from data_utils import get_all_stocks, load_all_data_60days, is_within_trading_hours, safe_float_conversion
from filters import filter_candidates, filter_stocks
from rabbitmq_handler import send_order_to_rabbitmq, start_account_info_consumer, get_account_info, request_account_info
from sell_strategy import get_sell_candidates
from stock_code_fetcher import fetch_and_save_stock_codes
from market_trend import calculate_market_trend  # 新增导入

logging.getLogger('pika').setLevel(logging.WARNING)

def stock_watcher_main():
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')
    field_mappings = dict(config['field_mappings'])
    fetch_and_save_stock_codes()
    existing_stocks = set()
    pending_orders = set()
    request_account_info()
    all_stocks = get_all_stocks()
    logging.info(f"共计 {len(all_stocks)} 只股票，开始加载近 60 天历史数据...")
    df_all = load_all_data_60days()
    logging.info(f"加载完毕，数据量: {len(df_all)} 条，开始进行技术形态筛选...")
    qualified_stocks_info = filter_candidates(df_all)
    logging.info(f"技术形态筛选后，共 {len(qualified_stocks_info)} 只股票符合，开始实时监控···")

    while True:
        schedule.run_pending()
        if is_within_trading_hours():
            start_time = time.time()
            try:
                all_realtime_data = ef.stock.get_realtime_quotes()
                all_realtime_data = all_realtime_data.rename(columns=field_mappings)
                market_trend = calculate_market_trend(all_realtime_data)  # 调用新函数

                if market_trend >= 0:
                    new_stocks = filter_stocks(all_realtime_data, qualified_stocks_info, existing_stocks)
                    for stock_code, _ in new_stocks:
                        if stock_code not in pending_orders:
                            logging.info(f"买入：{stock_code}")
                            # send_order_to_rabbitmq(stock_code, "buy")
                            pending_orders.add(stock_code)
                else:
                    logging.info("上涨家数不足30%，不执行买入操作")

                account_info = get_account_info()
                sell_candidates = get_sell_candidates(all_realtime_data, account_info)

                if market_trend < 30:
                    for account_id, holdings in account_info.items():
                        for holding in holdings or []:
                            stock_code = holding.get('证券代码')
                            stock_data = all_realtime_data[all_realtime_data['code'] == stock_code]
                            if not stock_data.empty:
                                price_change = safe_float_conversion(stock_data.iloc[0]['price_change_percent'])
                                if price_change is not None and price_change < 0:
                                    if (account_id, stock_code) not in sell_candidates:
                                        sell_candidates.append((account_id, stock_code))
                                        logging.info(f"市场趋势差，卖出持仓股票 {stock_code}，因涨跌幅 < 0")

                for account_id, stock_code in sell_candidates:
                    if stock_code not in pending_orders:
                        logging.info(f"(账户ID={account_id}) 卖出：{stock_code}")
                        send_order_to_rabbitmq(stock_code, "sell", account_id)
                        pending_orders.add(stock_code)

                holding_stocks = set()
                for holdings in account_info.values():
                    for stock in holdings or []:
                        holding_stocks.add(stock['证券代码'])
                pending_orders.intersection_update(holding_stocks)

                elapsed_time = time.time() - start_time
                print(f"实时数据共 {len(all_realtime_data)} 条，监控运行周期用时 {elapsed_time:.2f} 秒")
                time.sleep(max(3 - elapsed_time, 0))
            except Exception as e:
                logging.error(f"主函数执行过程中发生了错误: {e}")
                time.sleep(3)
        else:
            time.sleep(60)

if __name__ == "__main__":
    threading.Thread(target=start_account_info_consumer, daemon=True).start()
    stock_watcher_main()