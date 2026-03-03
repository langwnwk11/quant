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


def get_trade_days():
    # 1. 获取原始数据 (AkShare 默认返回列名为 'trade_date')
    trade_cal = ak.tool_trade_date_hist_sina()
   
    # 2. 转换类型
    trade_cal['trade_date'] = pd.to_datetime(trade_cal['trade_date'])
    
    # 3. 重命名列名为 'date'
    trade_cal = trade_cal.rename(columns={'trade_date': 'date'})
    
    # 4. 排序并重置索引
    trade_cal = trade_cal.sort_values('date').reset_index(drop=True)
    
     
    time.sleep(wait_time_seconds_after_execute)
        
    return trade_cal[['date']]

