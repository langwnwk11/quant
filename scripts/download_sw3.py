import sys
import time
import pandas as pd
import akshare as ak
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import io
from io import StringIO
import pandas as pd
import requests

def sw_index_third_cons_change(symbol: str = "801120.SI") -> pd.DataFrame:
    """
    乐咕乐股-申万三级-行业成份 (2026 修复自适应版)
    https://legulegu.com/stockdata/index-composition?industryCode=801120.SI
    :param symbol: 三级行业的行业代码
    :type symbol: str
    :return: 行业成份
    :rtype: pandas.DataFrame
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    url = f"https://legulegu.com/stockdata/index-composition?industryCode={symbol}"
    r = requests.get(url, headers=headers) # 保持你原本的 headers 不变
    
    # 1. 抓取网页表格
    temp_df = pd.read_html(StringIO(r.text))[0]
    
    # 2. 【核心优化】不再硬编码 columns，直接使用网页抓取到的最新列名
    # 为了避免有些列名带有动态日期导致后续难处理，我们做一个映射，只清洗我们关心的列
    current_cols = list(temp_df.columns)
    
    # 3. 基础数值类型转换（名称不变，直接按列名转）
    numeric_cols = ["价格", "市盈率", "市盈率ttm", "市净率"]
    for col in numeric_cols:
        if col in temp_df.columns:
            temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")
            
    # 特殊处理：市值（亿元）
    sz_col = [c for c in temp_df.columns if "市值" in c]
    if sz_col:
        temp_df[sz_col[0]] = pd.to_numeric(temp_df[sz_col[0]], errors="coerce")

    # 4. 百分比符号清洗与转换（利用 `.str.contains` 动态匹配，无惧未来日期变化）
    for col in temp_df.columns:
        # 匹配 股息率、各类涨幅、以及净利润/营业收入同比增长
        if any(keyword in col for keyword in ["涨幅", "股息率", "净利润", "营业收入"]):
            # 如果是字符串类型，则去掉 %
            if temp_df[col].dtype == "object":
                temp_df[col] = temp_df[col].str.strip("%")
            # 统一转为 float 数值
            temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")

    return temp_df
# 🔥 关键魔法：动态替换 AkShare 官方的接口
ak.sw_index_third_cons = sw_index_third_cons_change
# --- 配置 ---
ROOT_DIR = Path(__file__).parent.parent
OUTPUT_BASE = ROOT_DIR / "output"
BASE_DATA_DIR = ROOT_DIR / "data" / "baseData"
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
    # 动态适配传入的 Akshare 函数
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
def download_sw3_batch(codes):
    """核心下载逻辑：获取申万三级行业成份"""
    success_list = []
    
    # 设定存储子目录为 sw3_cons
    folder_name = "sw3_cons"
    (OUTPUT_BASE / folder_name).mkdir(parents=True, exist_ok=True)

    for code in codes:
        save_path = OUTPUT_BASE / folder_name / f"{code}.parquet"
        if save_path.exists():
            success_list.append(code)
            continue
        
        try:
            # 传入申万三级成份股接口
            df = fetch_api_data(ak.sw_index_third_cons, code)
            if df is not None:
                df.to_parquet(save_path)
                success_list.append(code)
            else:
                print(f"⚠️ 行业代码 {code} 返回数据为空")
            time.sleep(0.5)  # 控频防封
        except Exception as e:
            print(f"❌ 下载行业代码 {code} 失败: {e}")
            
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
        
        print(f"🔄 组别 {group_index} | 第 {i+1} 轮修复 | 剩余 {len(missing)} 个行业...")
        
        newly_success = download_sw3_batch(missing)
        
        # 合并并更新成功日志
        updated_success = success_now | set(newly_success)
        write_list_file(log_file, updated_success)
        
        # 最后一轮处理失败记录
        if i == MAX_REPAIR_ATTEMPTS - 1:
            final_missing = all_tasks - updated_success
            if final_missing:
                print(f"⚠️ 达到上限，失败 {len(final_missing)} 个，记录至 {fail_file.name}")
                write_list_file(fail_file, final_missing)
            elif fail_file.exists():
                fail_file.unlink()

def init_base_data():
    """初始化基础数据：若不存在则下载 sw3.parquet"""
    BASE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    sw3_file = BASE_DATA_DIR / "sw3.parquet"
    
    if not sw3_file.exists():
        print("🌐 未发现本地基础数据，开始从 AkShare 下载申万三级行业信息...")
        try:
            df = ak.sw_index_third_info()
            if df is not None and not df.empty:
                df.to_parquet(sw3_file)
                print(f"💾 基础数据已保存至: {sw3_file}")
            else:
                raise ValueError("获取的申万三级行业信息为空")
        except Exception as e:
            print(f"💥 初始化基础数据失败: {e}")
            sys.exit(1)
    return sw3_file

def load_sw3_codes():
    """从本地 parquet 加载去重后的行业代码"""
    sw3_file = init_base_data()
    df = pd.read_parquet(sw3_file)
    if "行业代码" not in df.columns:
        raise KeyError("parquet 文件中未找到 '行业代码' 列，请检查数据接口更新。")
    return df['行业代码'].dropna().unique().tolist()

def load_fail_list(group_index):
    """加载历史失败的任务清单（用于紧急重试任务）"""
    fail_file = PathHelper.get_path(group_index, "fail")
    if not fail_file.exists():
        return []
    return sorted(list(read_list_file(fail_file)))

def main(group_index, total_groups, task_type):
    # task_type 为 1 代表只读本组上次失败的对冲任务，否则读取全部新任务
    if task_type == 1:
        my_codes = load_fail_list(group_index)
        print(f"📋 组别 {group_index}: 模式 [修复失败任务]，待处理 {len(my_codes)} 个行业")
    else:
        all_codes = load_sw3_codes()
        print(f"📊 申万三级总行业数: {len(all_codes)}")
        
        # 科学切片：分到 20 组
        avg = len(all_codes) // total_groups
        start_idx = group_index * avg
        end_idx = (group_index + 1) * avg if group_index != total_groups - 1 else len(all_codes)
        my_codes = all_codes[start_idx:end_idx]
        print(f"🚀 组别 {group_index}: 模式 [全量分流]，分配任务 {len(my_codes)} 个行业")

    if not my_codes:
        print(f"🏁 组别 {group_index}: 没有需要执行的任务。")
        return

    # 保存本组任务清单
    write_list_file(PathHelper.get_path(group_index, "task"), my_codes)

    # 启动自愈下载
    repair_download(group_index)

if __name__ == "__main__":
    # 参数解释：脚本名 组别索引(0-19) 总组数(20) 任务类型(0:普通/1:仅修复历史失败)
    # main(0,20,0)
    if len(sys.argv) < 4:
        print("Usage: python script.py <GROUP_INDEX> <TOTAL_GROUPS> <TASK_TYPE>")
        print("Example: python script.py 0 20 0")
    else:
        main(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
        # main(0,20,0)