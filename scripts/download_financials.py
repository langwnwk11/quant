import os
import sys
import time
import pandas as pd
import akshare as ak
from pathlib import Path

# --- 配置区 ---
RETRIES = 3
SLEEP_TIME = 2  # 基础等待秒数

def download_data(group_index, total_groups):
    # 1. 路径设置
    root_dir = Path(__file__).parent.parent
    data_file = root_dir / "data" / "baseData" / "stock.parquet"
    output_base = root_dir / "output"
    
    # 创建必要的文件夹
    for sub in ["balance", "profit", "cash"]:
        (output_base / sub).mkdir(parents=True, exist_ok=True)

    # 2. 读取股票列表并分组
    if not data_file.exists():
        print(f"错误: 找不到数据文件 {data_file}")
        return

    df_stocks = pd.read_parquet(data_file)
    all_codes = df_stocks['code'].unique().tolist()
    # all_codes = all_codes[:5]
    # 简单的分桶逻辑
    avg = len(all_codes) // total_groups
    start_idx = group_index * avg
    # 最后一组处理余数
    end_idx = (group_index + 1) * avg if group_index != total_groups - 1 else len(all_codes)
    
    my_codes = all_codes[start_idx:end_idx]
    print(f"组别 {group_index}: 准备处理 {len(my_codes)} 只股票")

    # 3. 抓取逻辑
    success_log = []
    
    # 定义内部抓取函数带重试
    def fetch_with_retry(func, symbol):
        for i in range(RETRIES):
            try:
                # AkShare 接口通常需要 6 位代码
                data = func(symbol=symbol)
                if data is not None and not data.empty:
                    return data
                return None
            except Exception as e:
                wait = SLEEP_TIME * (i + 1)
                print(f"  [重试 {i+1}] {symbol} 接口错误: {e}, 等待 {wait}s...")
                time.sleep(wait)
        return None

    for code in my_codes:
        print(f"正在处理: {code}")
        try:
            # 对应的三个接口映射
            tasks = {
                "balance": ak.stock_balance_sheet_by_yearly_em,
                "profit": ak.stock_profit_sheet_by_yearly_em,
                "cash": ak.stock_cash_flow_sheet_by_yearly_em
            }
            
            is_all_ok = True
            for folder, api_func in tasks.items():
                save_path = output_base / folder / f"{code}.parquet"
                
                # 如果文件已存在则跳过 (可选)
                if save_path.exists():
                    continue
                
                df = fetch_with_retry(api_func, code)
                if df is not None:
                    df.to_parquet(save_path)
                else:
                    is_all_ok = False
                
                time.sleep(0.5) # 常规防爬间隔

            if is_all_ok:
                success_log.append(code)
                
        except Exception as e:
            print(f"处理 {code} 时发生未知错误: {e}")

    # 4. 写入成功记录
    log_file = output_base / f"{group_index}.txt"
    with open(log_file, "w") as f:
        for c in success_log:
            f.write(f"{c}\n")
    
    print(f"组别 {group_index} 处理完毕，成功记录至 {log_file}")

if __name__ == "__main__":
    # 示例用法: python scripts/download_financials.py 0 10
    if len(sys.argv) < 3:
        print("请传入参数: GROUP_INDEX TOTAL_GROUPS")
    else:
        g_idx = int(sys.argv[1])
        t_groups = int(sys.argv[2])
        download_data(g_idx, t_groups)