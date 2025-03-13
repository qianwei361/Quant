# 项目文件列表及功能说明

"""
- __init__.py                  初始化文件，用于将目录标识为 Python 包
- config.ini                  配置文件，存储数据库、路径、日志等设置
- data_utils.py               数据工具模块，包含加载股票数据、时间判断等工具函数
- filters.py                  过滤模块，包含筛选股票的技术指标逻辑
- main.py                     主程序入口，执行股票监控和交易逻辑
- market_trend.py             市场趋势模块，计算市场趋势（如上涨股票比例）
- rabbitmq_handler.py         消息队列处理模块，与 RabbitMQ 交互发送订单
- run_main.bat                批处理脚本，用于运行主程序
- run_update_and_upload.bat   批处理脚本，用于运行更新和上传任务
- sector_codes.json           行业代码文件，存储行业分类数据（JSON 格式）
- sell_strategy.py            卖出策略模块，定义卖出股票的逻辑
- stock_code_fetcher.py       股票代码获取模块，获取并保存股票代码列表
- stock_codes.json            股票代码文件，存储所有股票代码（JSON 格式）
- tmp.png                     临时图片文件，可能用于调试或记录
- trader.py                   交易核心模块，可能包含交易执行逻辑
- update_and_upload.py        更新和上传模块，处理数据更新和上传任务
"""