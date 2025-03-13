# trader_app.py
import pika
import json
import easytrader
import logging
import time
import schedule

# 设置日志格式
logging.getLogger('pika').setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# RabbitMQ 配置
RABBITMQ_HOST = '8.138.82.182'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'Quant'
RABBITMQ_PASSWORD = 'Qian_0822'
QUEUE_NAME_ORDER = 'stock_orders'
QUEUE_NAME_ACCOUNT = 'account_300323info'

# 账户标识符
ACCOUNT_ID = "95091066"

# 账户缓存
account_cache = {
    "账户ID": ACCOUNT_ID,
    "资金状况": {},
    "持仓状况": []
}

# 初始化券商客户端
def init_user():
    user = easytrader.use('gj_client')
    user.prepare(
        user=ACCOUNT_ID,
        password="qw123456",
        exe_path=r"C:\\同花顺远航版\\transaction\\xiadan.exe"
    )
    user.enable_type_keys_for_editor()
    return user

user = init_user()

# 获取持仓信息
def get_position():
    try:
        position = user.position
        if position is None:
            logging.error("获取持仓信息失败: 持仓数据返回 None")
            return []  # 确保返回空列表，而不是 None
        return position
    except Exception as e:
        logging.error(f"获取持仓信息失败: {e}")
        return []  # 返回空列表，避免 NoneType 问题


# 发送账户信息到 RabbitMQ，并更新缓存
def send_account_info():
    global account_cache
    try:
        balance = user.balance
        position = get_position()  # 这里改成调用 get_position()，避免 None

        # 打印持仓和资金信息
        print("账户资金状况:", balance)
        print("账户持仓状况:", position)

        # 更新缓存
        account_cache["资金状况"] = balance
        account_cache["持仓状况"] = position

        account_info = {
            '账户ID': ACCOUNT_ID,
            '资金状况': balance,
            '持仓状况': position
        }

        message = json.dumps(account_info)
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
        )
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME_ACCOUNT, durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME_ACCOUNT,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        logging.info(f"账户信息已发送, 账户ID={ACCOUNT_ID}")

        connection.close()
    except Exception as e:
        logging.error(f"发送账户信息时发生错误: {e}")


# 定时任务，定期发送账户信息
def schedule_task():
    schedule.every().day.at("09:25").do(send_account_info)
    while True:
        schedule.run_pending()
        time.sleep(1)


# 处理 RabbitMQ 订单指令（买入、卖出、查询持仓）
def callback(ch, method, properties, body):
    try:
        message = json.loads(body)
        action = message.get('action')
        stock_code = message.get('股票代码', None)
        account_id = message.get('账户ID', None)

        if not action:
            logging.warning(f"无效消息，缺少 action 字段，忽略: {message}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if action == "query_holdings":
            logging.info("收到持仓查询请求，正在查询...")
            send_account_info()  # 立即查询持仓信息并发送给生产者
        elif action == "buy" and stock_code:
            # 检查是否已持有该股票
            held_stocks = {pos['证券代码']: pos for pos in account_cache.get("持仓状况", [])}
            if stock_code in held_stocks and held_stocks[stock_code]['股票余额'] > 0:
                logging.info(f"已持有 {stock_code}，股票余额 {held_stocks[stock_code]['股票余额']}，跳过买入")
            else:
                logging.info(f"买入：{stock_code}")
                user.buy(stock_code, price=None, amount=None)
                logging.info(f"成功买入 {stock_code}")

        elif action == "sell" and stock_code and account_id == ACCOUNT_ID:
            logging.info(f"卖出：{stock_code}")
            user.sell(stock_code, price=None, amount=None)
            logging.info(f"成功卖出 {stock_code}")
        else:
            logging.warning(f"无效的账户ID或 action: {message}，消息被忽略")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logging.error(f"处理 RabbitMQ 消息时发生错误: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


# 启动 RabbitMQ 订单消费者
def start_consumer():
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
        )
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME_ORDER, durable=True)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=QUEUE_NAME_ORDER, on_message_callback=callback)

        logging.info("开始监听 `stock_orders` 队列...")
        channel.start_consuming()
    except Exception as e:
        logging.error(f"启动消费者失败: {e}")


if __name__ == "__main__":
    start_consumer()


