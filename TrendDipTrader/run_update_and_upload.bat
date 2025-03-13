@echo off
set PATH=C:\QuantTrader\.venv\Scripts;C:\Windows\System32;%PATH%
cd /d "C:\QuantTrader\TrendDipTrader"
"C:\QuantTrader\.venv\Scripts\python.exe" "C:\QuantTrader\TrendDipTrader\update_and_upload.py"
pause
