# rabbitmq_handler.py
import logging
import pika
import json
import time
import os
import configparser

# 读取配置
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"), encoding="utf-8")

RABBITMQ_HOST = config.get("rabbitmq", "host")
RABBITMQ_PORT = config.getint("rabbitmq", "port")
RABBITMQ_USER = config.get("rabbitmq", "user")
RABBITMQ_PASSWORD = config.get("rabbitmq", "password")
QUEUE_NAME = config.get("rabbitmq", "queue_orders")
ACCOUNT_INFO_QUEUE = config.get("rabbitmq", "queue_account_info")

logging.getLogger('pika').setLevel(logging.WARNING)

account_positions = {}


def send_order_to_rabbitmq(stock_code, action, account_id=None):
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        order_message = {"股票代码": stock_code,"action": action}
        if action == "sell" and account_id:
            order_message["账户ID"] = account_id
        channel.basic_publish(exchange='',routing_key=QUEUE_NAME,body=json.dumps(order_message),properties=pika.BasicProperties(delivery_mode=2))
        logging.info(f"{action.upper()} 请求已成功发送到 RabbitMQ: {stock_code} (账户ID={account_id if action == 'sell' else 'N/A'})")
        if action == "sell" and account_id:
            if account_id in account_positions:
                account_positions[account_id] = [stock for stock in account_positions[account_id] if stock["证券代码"] != stock_code]
                logging.info(f"已卖出 {stock_code}，从账户 {account_id} 的持仓缓存中删除")
        connection.close()
    except Exception as e:
        logging.error(f"发送 {action} 请求到 RabbitMQ 失败: {e}")


def request_account_info():
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        message = json.dumps({"action": "query_holdings"})
        channel.basic_publish(exchange='',routing_key=QUEUE_NAME,body=message,properties=pika.BasicProperties(delivery_mode=2))
        logging.info("已请求最新持仓信息")
        connection.close()
    except Exception as e:
        logging.error(f"请求账户持仓信息失败: {e}")


def account_info_callback(ch, method, properties, body):
    global account_positions
    try:
        account_data = json.loads(body)
        account_id = account_data.get('账户ID')
        position = account_data.get('持仓状况', [])
        if not account_id:
            logging.warning(f"接收到的账户信息缺少账户ID: {account_data}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        account_positions[account_id] = position
        logging.info(f"持仓信息更新成功, 账户ID={account_id}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except json.JSONDecodeError as e:
        logging.error(f"JSON 解析失败: {body}，错误: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        logging.error(f"处理账户信息时发生未知错误: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def start_account_info_consumer():
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials))
            channel = connection.channel()
            channel.queue_declare(queue=ACCOUNT_INFO_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=ACCOUNT_INFO_QUEUE, on_message_callback=account_info_callback)
            logging.info("开始实时监听账户信息更新...")
            channel.start_consuming()
        except Exception as e:
            logging.error(f"账户信息消费者异常: {e}，10秒后重试")
            time.sleep(10)


def get_account_info():
    return account_positions
