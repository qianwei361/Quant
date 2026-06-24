# 量化 UI 面板「指向真实数据」修复计划

## 背景 / 为什么做这件事

`Database/quant_trader_gui.py` 是 PyQt 面板，所有"提示信息"都来自后台线程的 `logging` 输出，
转发到窗口的 `log_display`。审计结论：

- **数据源本身是真实的**：efinance 实时行情、SQL Server `StockPriceHistory`、RabbitMQ、easytrader 实盘账户。全仓库无 mock/随机/占位数据。
- **但面板当前跑不起来**：导入了不存在的幽灵模块 `每日修改记录缓存.main`；GUI 在 `Database/` 却导入位于 `TrendDipTrader/` 的模块，`sys.path` 不通。
- **即便跑起来，部分提示信息会"失真"**：买入信号不真实下单、卖出因字段名错误恒失败、交易时间判断恒为真、账户队列名不一致导致持仓永不同步、市场趋势缺数据时回退为假值。

目标：让面板能真正启动，并让**每一条提示信息背后都是真实、可信的数据**。

已确认的关键决策：
1. **启用真实买入下单**（取消 main.py:46 的注释）。
2. **全面修复 + 冗余清理 + 配置统一**。
3. **也要能在本 Linux 容器启动**（实盘交易执行仍仅限 Windows）。

---

## A. 让面板能启动（导入 / 工作目录）

1. `Database/quant_trader_gui.py`
   - 顶部加入：把 `TrendDipTrader/` 加进 `sys.path`
     `sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "TrendDipTrader"))`
   - 修正三处导入：
     - `from 每日修改记录缓存.main import stock_watcher_main` → `from main import stock_watcher_main`
     - `from tradder import start_consumer` → 不变（路径已通）
     - `from update_and_upload import main` → 不变（路径已通）
   - `MonitorThread.run` 还需启动账户信息消费者（否则持仓缓存永远为空）：
     在 `stock_watcher_main()` 前 `threading.Thread(target=start_account_info_consumer, daemon=True).start()`
     （现状只有 `__main__` 里才起这个线程，GUI 路径漏了）。

2. 消除"工作目录相对路径"依赖 —— 改为按文件位置读 config（对齐 `data_utils.py`/`rabbitmq_handler.py` 已有写法）：
   - `TrendDipTrader/main.py:19`、`filters.py:86`、`stock_code_fetcher.py:14`
     `config.read('config.ini')` → `config.read(os.path.join(os.path.dirname(__file__), 'config.ini'), encoding='utf-8')`

## B. 配置统一 + 跨平台路径

3. `TrendDipTrader/config.ini` `[paths]`：Windows 绝对路径 → 文件名（相对模块目录）
   - `stock_codes = stock_codes.json`、`sector_codes = sector_codes.json`
   - 代码侧按 `__file__` 解析，避免 CWD 依赖：
     - `data_utils.py:24` `STOCK_CODES_PATH = os.path.join(os.path.dirname(__file__), config.get("paths","stock_codes"))`
     - `stock_code_fetcher.py`、`update_and_upload.py` 同样的输出/读取路径都按 `__file__` 解析。

4. 三处 `config.ini` 节名统一：`Database/config.ini` 现用大写 `[Stocks]`/`[Paths]` 且缺 `[rabbitmq]`，
   与 `StockTrader`/`TrendDipTrader` 的小写 `[database]`/`[paths]` 不一致。
   - 统一为小写 schema，并同步更新 `Database/` 下引用这些节的脚本（编号脚本 1/2/3 等）。
   - `Database/` 下脚本里的 `C:\QuantTrader\Database\...` 路径同样改为按 `__file__` 解析的相对路径。

## C. 修复让提示信息"失真"的数据流 bug

5. `TrendDipTrader/main.py:46`：**启用真实买入** —— 取消注释
   `send_order_to_rabbitmq(stock_code, "buy")`
   - 配套安全开关：新增 `[trading] enable_real_orders` 配置项，买入/卖出发单前判断该开关（默认 `true`，按你的选择；想停手只需改成 `false` 无需改代码）。

6. `TrendDipTrader/sell_strategy.py:23-24`：字段名按 `field_mappings` 修正
   - `getattr(stock, '代码')` → `getattr(stock, 'code')`
   - `getattr(stock, '最新价')` → `getattr(stock, 'latest_price')`
   - 修后卖出策略才能真正取到最新价，"卖出股票…盈亏比…"提示才真实。

7. `TrendDipTrader/data_utils.py:69-77` `is_within_trading_hours()`：改为 A 股真实交易时段
   - `weekday() < 5`（周一~周五），时段 09:30–11:30 与 13:00–15:00。

8. RabbitMQ 账户队列名不一致 —— 统一为 config 的 `account_info`
   - `TrendDipTrader/tradder.py:14-22`：RabbitMQ host/port/user/password、队列名、账户ID/密码/exe 路径全部改为从 `config.ini` 读取（对齐 `rabbitmq_handler.py` 写法），删除硬编码的 `account_300323info`。
   - 修后 tradder 发送的持仓信息才能被 `start_account_info_consumer` 收到，"持仓信息更新成功"才真实，卖出策略也才有真实持仓可用。

9. `TrendDipTrader/market_trend.py:18-20`：缺数据时不再返回假值 `0`
   - 返回 `None`；`main.py` 中 `if market_trend is None:` 跳过本轮买卖，避免基于占位数据交易。

10. 次要：`update_and_upload.py` 第二个 `except` 补上 `{e}` 错误详情，便于诊断。

## D. 冗余清理

11. 删除 `StockTrader/` 整个目录 —— 与 `TrendDipTrader` 功能重复且未被面板调用
    （`QuantStockFilter.py` ≈ `update_and_upload.py`，`DatabaseSynchronizer.py` ≈ `Database/3将板块和股票数据上传到数据库.py`）。
    > 删除前会再次确认；如你想保留作备份，可改为移动到 `archive/`。

---

## 验证方式

- **本容器（导入级冒烟测试，不依赖实盘/SQL Server/网络）**：
  - `python -c "import sys,os; sys.path.insert(0,'TrendDipTrader'); import main, tradder, update_and_upload, sell_strategy, filters, market_trend, data_utils, rabbitmq_handler"` 确认无 `ModuleNotFoundError` / 节名 `KeyError`。
  - 若装了 PyQt6：`QT_QPA_PLATFORM=offscreen python Database/quant_trader_gui.py` 能实例化窗口不崩（无显示器用 offscreen）。
  - 单测 `is_within_trading_hours()` 在工作日盘中/盘后、周末分别返回正确布尔值。
- **Windows 实盘（你本机，真实验证）**：启动面板 → 点"启动监控" → 观察日志依次出现
  "共计 X 只股票…/加载完毕 X 条/技术形态筛选 X 只/开始实时监控"，且买卖、持仓提示对应真实账户。
  ⚠️ 因已启用真实下单，请先用极小资金或盘后验证。

## 提交

- 分支 `claude/quant-ui-panel-data-mdcvji`，按上面 A/B/C/D 分组提交，推送后建草稿 PR。
