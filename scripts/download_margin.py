import os
import sys
import argparse
import pandas as pd
import akshare as ak
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# --- 1. 配置管理 ---
def get_paths():
    """定义项目路径"""
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return {
        "data_path": os.path.join(root_dir, "data", "baseData", "trade_days.parquet"),
        "output_dir": os.path.join(root_dir, "output"),
        "fail_log": os.path.join(root_dir, "output", "{}_fail.txt"),
        "result_pqt": os.path.join(root_dir, "output", "{}_margin.parquet")
    }

# --- 2. 日期处理模块 ---
def get_task_dates(data_path, start_date_str, group_index, total_groups):
    """
    读取交易日文件，并根据输入的起始日期进行过滤和分组
    """
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"未找到基础数据文件: {data_path}")

    df = pd.read_parquet(data_path)
    df['date'] = pd.to_datetime(df['date'])
    
    # 确定过滤范围：从输入日期（或默认2025-01-01）到今天
    start_dt = pd.to_datetime(start_date_str)
    end_dt = datetime.now()
    
    mask = (df['date'] >= start_dt) & (df['date'] <= end_dt)
    target_dates = sorted(df.loc[mask, 'date'].dt.strftime('%Y%m%d').tolist())
    
    if not target_dates:
        print(f"⚠️ 在范围 {start_date_str} 至 {end_dt.date()} 内没有找到交易日。")
        return []

    # 分组切片
    my_dates = target_dates[group_index::total_groups]
    return my_dates

# --- 3. 数据抓取模块 ---
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True
)
def fetch_single_day(date_str):
    """调用 akshare 接口，带 5 次指数退避重试"""
    # 接口文档：stock_margin_detail_szse
    df = ak.stock_margin_detail_szse(date=date_str)
    if df is not None and not df.empty:
        df['trade_date'] = date_str
    return df

# --- 4. 持久化模块 ---
def save_results(dfs, failed_dates, paths, group_index):
    """保存结果文件与失败记录"""
    os.makedirs(paths["output_dir"], exist_ok=True)
    
    # 合并并存为 Parquet
    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
        out_path = paths["result_pqt"].format(group_index)
        final_df.to_parquet(out_path, index=False)
        print(f"✅ 成功保存数据 ({len(dfs)}天) 至: {out_path}")
    
    # 记录失败日期
    fail_path = paths["fail_log"].format(group_index)
    if failed_dates:
        with open(fail_path, "w") as f:
            f.writelines(f"{d}\n" for d in failed_dates)
        print(f"❌ 最终失败日期已记录至: {fail_path}")
    elif os.path.exists(fail_path):
        os.remove(fail_path)

# --- 5. 执行流程控制 ---
def run_pipeline(args):
    paths = get_paths()
    
    # 1. 获取分配给本组的日期
    try:
        my_dates = get_task_dates(
            paths["data_path"], 
            args.date, 
            args.group_index, 
            args.total_groups
        )
    except Exception as e:
        print(f"初始化日期失败: {e}")
        return

    if not my_dates:
        return

    print(f"🚀 任务启动 | 组索引: {args.group_index} | 总组数: {args.total_groups}")
    print(f"📅 起始日期: {args.date} | 本组任务量: {len(my_dates)} 天")

    success_dfs = []
    failed_dates = []

    # 2. 遍历执行
    for date in my_dates:
        try:
            print(f"正在抓取 [{args.group_index}]: {date}...")
            data = fetch_single_day(date)
            if data is not None and not data.empty:
                success_dfs.append(data)
            else:
                print(f"ℹ️ 日期 {date} 无数据返回 (可能是非交易日或接口空值)")
        except Exception as e:
            print(f"🔥 日期 {date} 彻底失败: {e}")
            failed_dates.append(date)

    # 3. 保存结果
    save_results(success_dfs, failed_dates, paths, args.group_index)

# --- 6. 命令行参数入口 ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="分组合并抓取深交所两融数据")
    
    # 必填参数
    parser.add_argument("group_index", type=int, help="当前小组索引 (从0开始)")
    parser.add_argument("total_groups", type=int, help="总小组数")
    
    # 可选参数
    parser.add_argument(
        "--date", 
        type=str, 
        default="20250101", 
        help="起始日期，格式 YYYYMMDD (默认: 20250101)"
    )

    args = parser.parse_args()
    run_pipeline(args)