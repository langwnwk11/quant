import common_utils as funcutils
import pandas as pd
import os
# 1. 定位项目根目录 (scripts 的上一级)
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DATA_DIR = os.path.join(ROOT_DIR, "data/baseData")
if not os.path.exists(BASE_DATA_DIR):
    os.makedirs(BASE_DATA_DIR)
# 获取a股所有上市公司的代码和名称
def get_stock_list():
    
    temp=funcutils.get_sz_stock('A股列表')
    temp=temp[['A股代码',  'A股简称']]
    temp = temp.rename(columns={'A股代码': 'code', 'A股简称': 'name'})
    temp['code'] = 'sz' + temp['code'].astype(str)

    temp1 =funcutils.get_sh_stock('主板A股')
    temp2 =funcutils.get_sh_stock('科创板')
    df = pd.concat([temp1, temp2], ignore_index=True)
    df = df[['证券代码', '证券简称']]
    df = df.rename(columns={'证券代码': 'code', '证券简称': 'name'})
    df['code'] = 'sh' + df['code'].astype(str)

    stock = pd.concat([temp, df], ignore_index=True)
    file_path = os.path.join(BASE_DATA_DIR, "stock.parquet")
    stock.to_parquet(file_path, engine='pyarrow',index=False)
#保存a股交易日历数据
def save_trade_days():
    df=funcutils.get_trade_days()      
    file_path = os.path.join(BASE_DATA_DIR, "trade_days.parquet")
    df.to_parquet(file_path, engine='pyarrow',index=False)
    

if __name__ == "__main__":
    get_stock_list()
    save_trade_days()
     
