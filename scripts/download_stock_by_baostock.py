# 利用baostock 下载股票日线数据
import os
import argparse
import pandas as pd
import baostock as bs
from datetime import datetime, timedelta, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# --- 1. 配置管理 ---
def get_paths():
    """定义项目路径"""
    # 假设脚本在 scripts 目录下，获取项目根目录
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return {
        "base_stock_path": os.path.join(root_dir, "data", "baseData", "stock.parquet"),
        "output_dir": os.path.join(root_dir, "output"),
        "fail_log": os.path.join(root_dir, "output", "{}_fail.txt"),
        "result_pqt": os.path.join(root_dir, "output", "{}_stock.parquet")
    }

# --- 2. 逻辑处理模块 ---

def get_beijing_time():
    """获取当前北京时间"""
    # 使用 timezone 确保跨平台一致性
    tz_utc_8 = timezone(timedelta(hours=8))
    return datetime.now(tz_utc_8)

def get_end_date():
    """
    Step 2: 生成结束时间
    如果当前北京时间 < 17点，取前一天；否则取当天。
    """
    now_beijing = get_beijing_time()
    if now_beijing.hour < 17:
        end_dt = now_beijing - timedelta(days=1)
    else:
        end_dt = now_beijing
    return end_dt.strftime('%Y-%m-%d')

def get_stock_list(path, group_index, total_groups):
    """
    Step 1: 读取并处理 code，进行分组
    将 'sz000001' 转换为 'sz.000001'
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"未找到股票基础数据: {path}")
    
    df = pd.read_parquet(path)
    # 转换格式: sz000001 -> sz.000001
    codes = df['code'].apply(lambda x: f"{x[:2]}.{x[2:]}").tolist()
    
    # 分组切片
    my_codes = codes[group_index::total_groups]
    return my_codes

# --- 3. 数据抓取模块 ---

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True
)
def fetch_stock_k_data(code, start_date, end_date):
    """
    Step 3: 调用 baostock 接口获取数据并进行格式化处理
    """
    rs = bs.query_history_k_data_plus(
        code,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
        start_date=start_date, 
        end_date=end_date,
        frequency="d", 
        adjustflag="1"  # 1:后复权, 2:前复权, 3:不复权
    )
    
    if rs.error_code != '0':
        raise Exception(f"Baostock error: {rs.error_msg}")
    
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    
    if not data_list:
        return pd.DataFrame()
    
    # 转为 DataFrame
    df = pd.DataFrame(data_list, columns=rs.fields)

    # --- 数据格式化处理 ---
    
    # 1. 处理日期: 2025-01-01 -> 20250101
    df['date'] = df['date'].str.replace("-", "", regex=False)
    
    # 2. 处理代码: sz.000001 -> 000001
    # 解释：slice(3) 表示从索引3开始截取到最后，跳过 's', 'z', '.'
    df['code'] = df['code'].str.slice(3)
    
    # 3. 类型转换（可选）
    # Baostock 返回的通常是 object (string)，建议将数值列转为 float/int
    numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    return df
# --- 4. 执行流程控制 ---

def run_pipeline(args):
    paths = get_paths()
    os.makedirs(paths["output_dir"], exist_ok=True)
    
    # 1. 初始化 Baostock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock 登录失败: {lg.error_msg}")
        return

    try:
        # 2. 获取分组后的股票列表
        my_codes = get_stock_list(paths["base_stock_path"], args.group_index, args.total_groups)
        my_codes= my_codes[:10]  # 测试阶段先限制数量，正式运行时注释掉
        end_date = get_end_date()
        
        print(f"🚀 任务启动 | 组: {args.group_index}/{args.total_groups} | 结束日期: {end_date}")
        
        success_dfs = []
        failed_codes = []

        # 3. 遍历抓取
        for code in my_codes:
            try:
                print(f"正在抓取 {code} ...")
                df = fetch_stock_k_data(code, args.begin_date, end_date)
                if not df.empty:
                    success_dfs.append(df)
            except Exception as e:
                print(f"❌ {code} 抓取失败: {e}")
                failed_codes.append(code)

        # 4. 保存结果
        # 保存 Parquet
        if success_dfs:
            final_df = pd.concat(success_dfs, ignore_index=True)
            out_path = paths["result_pqt"].format(args.group_index)
            final_df.to_parquet(out_path, index=False)
            print(f"✅ 数据已保存至: {out_path}")

        # 保存失败记录
        fail_path = paths["fail_log"].format(args.group_index)
        if failed_codes:
            with open(fail_path, "w") as f:
                f.writelines(f"{c}\n" for c in failed_codes)
            print(f"⚠️ 失败代码记录至: {fail_path}")
        elif os.path.exists(fail_path):
            os.remove(fail_path)

    finally:
        bs.logout()

# --- 5. 命令行入口 ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Baostock 股票数据分组下载器")
    parser.add_argument("group_index", type=int, help="组索引")
    parser.add_argument("total_groups", type=int, help="总组数")
    parser.add_argument("begin_date", type=str, help="开始日期 (YYYY-MM-DD)")

    args = parser.parse_args()

    # --- 新增日期格式化处理逻辑 ---
    raw_date = args.begin_date.replace("-", "")  # 先去掉可能存在的横杠，统一成 YYYYMMDD
    if len(raw_date) == 8:
        # 将 20250101 转换为 2025-01-01
        formatted_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
        args.begin_date = formatted_date
    else:
        # 如果长度不对，抛出错误提示
        raise ValueError(f"无效的时间格式: {args.begin_date}，请输入 YYYYMMDD")
    run_pipeline(args)