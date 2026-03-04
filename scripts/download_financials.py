import sys
import time
import pandas as pd
import akshare as ak
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# --- 配置 ---
ROOT_DIR = Path(__file__).parent.parent
OUTPUT_BASE = ROOT_DIR / "output"
MAX_REPAIR_ATTEMPTS = 5  # 最大修复轮次

# Tenacity 限流与重试策略
ak_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=False # 即使失败也不抛出异常，交给业务逻辑处理
)

@ak_retry
def fetch_api_data(api_func, code):
    """原子操作：调用 AkShare 接口"""
    df = api_func(symbol=code)
    return df if (df is not None and not df.empty) else None

def save_task_codes(group_index, codes):
    """函数 1: 保存初始任务清单"""
    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
    file_path = OUTPUT_BASE / f"{group_index}_codes.txt"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("\n".join(codes))
    print(f"任务清单已保存: {file_path}")

def download_report_batch(codes):
    """函数 2: 核心下载逻辑"""
    success_list = []
    tasks = {
        "balance": ak.stock_balance_sheet_by_yearly_em,
        "profit": ak.stock_profit_sheet_by_yearly_em,
        "cash": ak.stock_cash_flow_sheet_by_yearly_em
    }
    
    # 确保子目录存在
    for sub in tasks.keys():
        (OUTPUT_BASE / sub).mkdir(parents=True, exist_ok=True)

    for code in codes:
        is_all_ok = True
        for folder, api_func in tasks.items():
            save_path = OUTPUT_BASE / folder / f"{code}.parquet"
            if save_path.exists():
                continue
            
            try:
                df = fetch_api_data(api_func, code)
                if df is not None:
                    df.to_parquet(save_path)
                else:
                    is_all_ok = False
                time.sleep(0.5) # 基础防爬
            except:
                is_all_ok = False
        
        if is_all_ok:
            success_list.append(code)
    return success_list

def repair_download(group_index):
    """函数 3: 自愈函数。比对文件，差额补全，最多循环 5 次"""
    task_file = OUTPUT_BASE / f"{group_index}_codes.txt"
    log_file = OUTPUT_BASE / f"{group_index}.txt"
    fail_file = OUTPUT_BASE / f"{group_index}_fail.txt" # 新增：失败记录文件
    
    final_success_set = set()

    for i in range(MAX_REPAIR_ATTEMPTS):
        # 1. 读取原始任务
        with open(task_file, "r") as f:
            all_task = set(line.strip() for line in f if line.strip())
        
        # 2. 读取当前已成功列表
        success_now = set()
        if log_file.exists():
            with open(log_file, "r") as f:
                success_now = set(line.strip() for line in f if line.strip())

        # 3. 找到待补下载的名单
        missing = list(all_task - success_now)
        
        if not missing:
            print(f"✅ 组别 {group_index}: 所有股票已下载成功。")
            final_success_set = all_task
            # 如果之前有失败文件，现在成功了，可以删除旧的失败文件
            if fail_file.exists(): fail_file.unlink()
            break
        
        print(f"🔄 正在进行第 {i+1} 轮修复，剩余 {len(missing)} 只股票...")
        
        # 4. 执行补抓
        newly_success = download_report_batch(missing)
        
        # 5. 更新成功日志
        final_success_set = success_now | set(newly_success)
        updated_success_list = sorted(list(final_success_set))
        with open(log_file, "w") as f:
            f.write("\n".join(updated_success_list))
        
        # --- 最后一轮结束后的处理 ---
        if i == MAX_REPAIR_ATTEMPTS - 1:
            final_missing = sorted(list(all_task - final_success_set))
            if final_missing:
                print(f"⚠️ 已达最大重试次数，共有 {len(final_missing)} 只股票下载失败。记录至 {fail_file.name}")
                with open(fail_file, "w", encoding="utf-8") as f:
                    f.write("\n".join(final_missing))
            else:
                if fail_file.exists(): fail_file.unlink()
def main(group_index, total_groups):
    # 1. 加载全量数据并分组
    data_file = ROOT_DIR / "data" / "baseData" / "stock.parquet"
    if not data_file.exists(): return
    
    all_codes = pd.read_parquet(data_file)['code'].unique().tolist()
    # all_codes=all_codes[0:5]
    avg = len(all_codes) // total_groups
    start_idx = group_index * avg
    end_idx = (group_index + 1) * avg if group_index != total_groups - 1 else len(all_codes)
    my_codes = all_codes[start_idx:end_idx]

    # 2. 保存任务清单
    save_task_codes(group_index, my_codes)

    # 3. 启动自愈下载流程 (内部会调用核心下载函数并处理重试)
    repair_download(group_index)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("参数缺失: GROUP_INDEX TOTAL_GROUPS")
    else:
        main(int(sys.argv[1]), int(sys.argv[2]))