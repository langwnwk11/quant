import os
import time 
import pandas as pd
import akshare as ak
from tenacity import retry, stop_after_attempt, wait_fixed

wait_time_seconds_after_execute = 2
# 获取沪市股票列表 ，重试3次，每次间隔2秒
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_sh_stock(symbol):
  df = ak.stock_info_sh_name_code(symbol=symbol)
  time.sleep(wait_time_seconds_after_execute)
  return df
# 获取深市股票列表
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_sz_stock(symbol):
  df = ak.stock_info_sz_name_code(symbol=symbol)
  time.sleep(wait_time_seconds_after_execute)
  return df

def get_trade_days( ):
  trade_cal = ak.tool_trade_date_hist_sina()
    # 确保 'trade_date' 列是 datetime 类型
  trade_cal['trade_date'] = pd.to_datetime(trade_cal['trade_date'])
  # 转换为字符串列表方便匹配
  trade_days = trade_cal['trade_date'].dt.strftime('%Y%m%d').tolist()
  # target_days = [d for d in trade_days if start_date <= d <= end_date]
  time.sleep(wait_time_seconds_after_execute)
  return trade_days