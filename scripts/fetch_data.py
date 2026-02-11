import pandas as pd
import akshare as ak
import time
import os
import sys
from tenacity import retry, stop_after_attempt, wait_fixed

# 这里的 group_index 和 total_groups 由 GitHub Action 传入
GROUP_INDEX = int(sys.argv[1])
TOTAL_GROUPS = 10

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_company_data(symbol):
    """获取三大报表，必须全部成功"""
    # 接口调用
    df_balance = ak.stock_balance_sheet_by_yearly_em(symbol=symbol)
    time.sleep(5) # 基础限流
    df_profit = ak.stock_profit_sheet_by_yearly_em(symbol=symbol)
    time.sleep(5)
    df_cash = ak.stock_cash_flow_sheet_by_yearly_em(symbol=symbol)
    
    return df_balance, df_profit, df_cash

def run():
    # 1. 加载股票列表并分组
    df_list = pd.read_parquet('stock.parquet')
    # 简单的取模分组
    df_list['group'] = range(len(df_list))
    df_list['group'] = df_list['group'] % TOTAL_GROUPS
    current_group = df_list[df_list['group'] == GROUP_INDEX]

    if not os.path.exists('output'): os.makedirs('output')
    if not os.path.exists('log'): os.makedirs('log')

    for symbol in current_group['code']:
        try:
            b, p, c = get_company_data(symbol)
            # 2. 存储为 HDF5
            file_path = f"output/{symbol}.h5"
            with pd.HDFStore(file_path, mode='w') as store:
                store.put('balance', b, format='table')
                store.put('profit', p, format='table')
                store.put('cash', c, format='table')
            print(f"Success: {symbol}")
        except Exception as e:
            with open(f"log/failed_group_{GROUP_INDEX}.log", "a") as f:
                f.write(f"{symbol}: {str(e)}\n")
            print(f"Failed: {symbol}")
        
        time.sleep(6) # 组内严格限流，避免被封 IP

if __name__ == "__main__":
    run()
