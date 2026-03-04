import sys
import time
import pandas as pd
import akshare as ak
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# --- 配置 ---
ROOT_DIR = Path(__file__).parent.parent
OUTPUT_BASE = ROOT_DIR / "output"
MAX_REPAIR_ATTEMPTS = 5

class PathHelper:
    """统一管理文件命名逻辑"""
    @staticmethod
    def get_path(group_idx, file_type):
        # 统一命名规则: group_{idx}_{type}.txt
        mapping = {
            "task": f"group_{group_idx}_task.txt",
            "log": f"group_{group_idx}_success.txt",
            "fail": f"group_{group_idx}_fail.txt"
        }
        return OUTPUT_BASE / mapping[file_type]

# --- 工具函数 ---
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True
)
def fetch_api_data(api_func, code):
    """原子操作：调用 AkShare 接口"""
    df = api_func(symbol=code)
    return df if (df is not None and not df.empty) else None

def read_list_file(path):
    """通用：读取文本列表"""
    if not path.exists():
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def write_list_file(path, data_list):
    """通用：保存文本列表"""
    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(list(data_list))))

# --- 核心业务 ---
def download_report_batch(codes):
    """核心下载逻辑"""
    success_list = []
    tasks = {
        "balance": ak.stock_balance_sheet_by_yearly_em,
        "profit": ak.stock_profit_sheet_by_yearly_em,
        "cash": ak.stock_cash_flow_sheet_by_yearly_em
    }
    
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
                time.sleep(0.5) 
            except:
                is_all_ok = False
        
        if is_all_ok:
            success_list.append(code)
    return success_list

def repair_download(group_index):
    """自愈函数：比对文件，差额补全"""
    task_file = PathHelper.get_path(group_index, "task")
    log_file = PathHelper.get_path(group_index, "log")
    fail_file = PathHelper.get_path(group_index, "fail")
    
    all_tasks = read_list_file(task_file)
    if not all_tasks:
        print(f"❌ 组别 {group_index}: 未发现任务清单。")
        return

    for i in range(MAX_REPAIR_ATTEMPTS):
        success_now = read_list_file(log_file)
        missing = list(all_tasks - success_now)
        
        if not missing:
            print(f"✅ 组别 {group_index}: 全部成功。")
            if fail_file.exists(): fail_file.unlink()
            break
        
        print(f"🔄 组别 {group_index} | 第 {i+1} 轮修复 | 剩余 {len(missing)} 只...")
        
        newly_success = download_report_batch(missing)
        
        # 合并并更新成功日志
        updated_success = success_now | set(newly_success)
        write_list_file(log_file, updated_success)
        
        # 最后一轮处理失败记录
        if i == MAX_REPAIR_ATTEMPTS - 1:
            final_missing = all_tasks - updated_success
            if final_missing:
                print(f"⚠️ 达到上限，失败 {len(final_missing)} 只，记录至 {fail_file.name}")
                write_list_file(fail_file, final_missing)
            elif fail_file.exists():
                fail_file.unlink()

def load_stock_codes():
    """加载股票代码列表"""
    data_file = ROOT_DIR / "data" / "baseData" / "stock.parquet"
    if not data_file.exists():
        print("数据文件不存在")
        return []
    
    return pd.read_parquet(data_file)['code'].unique().tolist()

def load_fail_list():
    """加载任务清单"""
    data_file = ROOT_DIR / "log" / "financial" / "fail.txt"
    if not data_file.exists():
        return []
    codes = sorted(list(read_list_file(data_file)))
    return codes

def main(group_index, total_groups,task_type):
    if(task_type == 1):
        all_codes = load_fail_list() 
        # print(all_codes)       
    else:
        all_codes = load_stock_codes()     
    all_codes = all_codes[:5]  # 测试阶段限制数量
    print(all_codes)
    avg = len(all_codes) // total_groups
    start_idx = group_index * avg
    end_idx = (group_index + 1) * avg if group_index != total_groups - 1 else len(all_codes)
    my_codes = all_codes[start_idx:end_idx]

    # 2. 保存任务清单 (使用统一命名)
    write_list_file(PathHelper.get_path(group_index, "task"), my_codes)

    # 3. 启动自愈下载
    repair_download(group_index)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python script.py <GROUP_INDEX> <TOTAL_GROUPS>")
    else:
        main(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
