import sys
from PyQt6.QtWidgets import QApplication, QMainWindow, QTextEdit, QPushButton, QVBoxLayout, QWidget
from PyQt6.QtCore import QThread, pyqtSignal, QTimer
import logging

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("量化交易系统")
        self.setGeometry(100, 100, 800, 600)

        # 创建控件
        self.log_display = QTextEdit()
        self.log_display.setReadOnly(True)
        self.start_button = QPushButton("启动监控")
        self.stop_button = QPushButton("停止监控")
        self.stop_button.setEnabled(False)

        # 布局
        layout = QVBoxLayout()
        layout.addWidget(self.start_button)
        layout.addWidget(self.stop_button)
        layout.addWidget(self.log_display)
        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

        # 连接按钮事件
        self.start_button.clicked.connect(self.start_monitoring)
        self.stop_button.clicked.connect(self.stop_monitoring)

        # 初始化线程和定时器
        self.monitor_thread = None
        self.consumer_thread = None
        self.update_timer = QTimer(self)
        self.update_timer.timeout.connect(self.update_database)
        self.update_timer.start(24 * 60 * 60 * 1000)  # 每天执行一次（单位：毫秒）

    def start_monitoring(self):
        # 启动生产者线程
        if not self.monitor_thread:
            self.monitor_thread = MonitorThread()
            self.monitor_thread.log_signal.connect(self.update_log)
            self.monitor_thread.start()

        # 启动消费者线程
        if not self.consumer_thread:
            self.consumer_thread = ConsumerThread()
            self.consumer_thread.log_signal.connect(self.update_log)
            self.consumer_thread.start()

        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.update_log("监控和交易已启动...")

    def stop_monitoring(self):
        if self.monitor_thread:
            self.monitor_thread.stop()
            self.monitor_thread = None
        if self.consumer_thread:
            self.consumer_thread.stop()
            self.consumer_thread = None
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.update_log("监控和交易已停止.")

    def update_database(self):
        from update_and_upload import main
        logging.basicConfig(level=logging.INFO)
        handler = LoggingHandler(self.update_log)
        logging.getLogger().addHandler(handler)
        self.update_log("开始更新数据库...")
        main()
        self.update_log("数据库更新完成.")

    def update_log(self, message):
        self.log_display.append(message)

class MonitorThread(QThread):
    log_signal = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.running = True

    def run(self):
        from 每日修改记录缓存.main import stock_watcher_main
        logging.basicConfig(level=logging.INFO)
        handler = LoggingHandler(self.log_signal)
        logging.getLogger().addHandler(handler)
        stock_watcher_main()

    def stop(self):
        self.running = False
        self.terminate()  # 强制终止线程（注意：实际使用时应优雅退出）

class ConsumerThread(QThread):
    log_signal = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.running = True

    def run(self):
        from tradder import start_consumer
        logging.basicConfig(level=logging.INFO)
        handler = LoggingHandler(self.log_signal)
        logging.getLogger().addHandler(handler)
        start_consumer()

    def stop(self):
        self.running = False
        self.terminate()  # 强制终止线程（注意：实际使用时应优雅退出）

class LoggingHandler(logging.Handler):
    def __init__(self, signal):
        super().__init__()
        self.signal = signal

    def emit(self, record):
        msg = self.format(record)
        self.signal(msg)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())