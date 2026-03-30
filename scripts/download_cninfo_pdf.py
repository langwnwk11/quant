import argparse
import os
import requests
import time
import random
from tqdm import tqdm
from pathlib import Path
import pandas as pd 
# --- 1. 配置管理 ---
def get_paths():
    """定义项目路径"""
    # 假设脚本在 scripts 目录下，获取项目根目录
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return {
        "base_stock_path": os.path.join(root_dir, "data", "baseData", "stock.parquet"),
        "finace_stock_path": os.path.join(root_dir, "data", "baseData", "unique_security_codes1.parquet"),
        "output_dir": os.path.join(root_dir, "output"),
        "fail_log": os.path.join(root_dir, "output", "{}_fail.txt"),
        "result_pqt": os.path.join(root_dir, "output", "{}_stock.parquet")
    }
def get_stock_list(path, group_index, total_groups):
    """
    Step 1: 读取并处理 code，进行分组
    将 'sz000001' 转换为 'sz.000001'
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"未找到股票基础数据: {path}")
    
    df = pd.read_parquet(path )
    
    codes = df['SECURITY_CODE'].tolist()
    print(f"总股票数量: {len(codes)}")
    # 分组切片
    my_codes = codes[group_index::total_groups]
    return my_codes
def make_safe_filename(s: str) -> str:
    return "".join(c for c in s if c.isalnum() or c in " -_").strip()

def fetch_cninfo_annual_reports(stock_code, org_id, download_dir):
    url = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"https://www.cninfo.com.cn/new/disclosure/stock?stockCode={stock_code}&orgId={org_id}"
    }
    column = "sse" if stock_code.startswith("6") else "szse"
    params = {
        "stock": f"{stock_code},{org_id}",
        "tabName": "fulltext",
        "column": column,
        "pageSize": 30,
        "pageNum": 1,
        "category": "category_ndbg_szsh",  # 年度报告
        "searchkey": "",
        "secid": "",
        "sortName": "",
        "sortType": "",
        "isHLtitle": "true"
    }
    os.makedirs(download_dir, exist_ok=True)
    all_reports = []
    stock_name = None  # 用于存储股票名称
    
    while True:
        try:
            print(f"正在请求第 {params['pageNum']} 页...")
            resp = requests.post(url, headers=headers, data=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            print(f"API响应状态: {resp.status_code}")
            print(f"API响应内容: {data}")
            anns = data.get("announcements", [])
            if not anns:
                print("没有更多公告")
                break
                
            # 获取股票名称（从第一条公告中获取）
            if not stock_name and anns:
                stock_name = anns[0].get("secName", "")
                if stock_name:
                    # 更新下载目录名称
                    # new_download_dir = f"./{stock_name}_{stock_code}_年报"
                    new_download_dir = os.path.join(paths["output_dir"], f"{stock_name}_{stock_code}_年报")
                    if new_download_dir != download_dir:
                        download_dir = new_download_dir
                        os.makedirs(download_dir, exist_ok=True)
                        print(f"创建下载目录: {download_dir}")
            
            for ann in anns:
                title = ann.get("announcementTitle", "")
                print(f"处理公告: {title}")
                # 只下载年度报告，不下载摘要
                is_annual_report = "年度报告" in title
                is_summary = "摘要" in title
                is_supplement = "补充" in title
                is_pdf = ann.get("adjunctUrl", "").upper().endswith(".PDF")
                
                print(f"  检查结果: 年度报告={is_annual_report}, 摘要={is_summary}, 补充={is_supplement}, PDF={is_pdf}")
                
                if (is_annual_report and 
                    not is_summary and 
                    not is_supplement and 
                    is_pdf):
                    pdf_url = "https://static.cninfo.com.cn/" + ann["adjunctUrl"]
                    fname = f"{stock_code}_{stock_name}_{make_safe_filename(title)}.pdf"
                    all_reports.append((pdf_url, fname))
                    print(f"添加报告: {fname}")
            if not data.get("hasMore", False):
                print("没有更多页面")
                break
            params["pageNum"] += 1
        except Exception as e:
            print(f"请求出错: {e}")
            break

    print(f"共找到 {len(all_reports)} 份年度报告，开始下载 …")
    session = requests.Session()
    for url, fname in tqdm(all_reports, desc="Downloading PDFs"):
        dest = os.path.join(download_dir, fname)
        if os.path.exists(dest):
            continue
        try:
            # 添加随机延迟，模拟人类行为
            delay = random.uniform(2, 5)  # 随机延迟2-5秒
            time.sleep(delay)
            
            r = session.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            with open(dest, "wb") as f:
                f.write(r.content)
        except Exception as e:
            print(f"[下载失败] {url} → {e}")
    print("全部下载完成！")

def get_org_id(stock_code):
    """根据股票代码获取orgId"""
    stock_code = stock_code.strip()
    if len(stock_code) == 6:
        # 深/沪 A 股
        urls = [
            ("szse", "https://www.cninfo.com.cn/new/data/szse_stock.json"),
            ("sse", "https://www.cninfo.com.cn/new/data/sse_stock.json")
        ]
    elif len(stock_code) == 5:
        # 港股
        urls = [("hke", "https://www.cninfo.com.cn/new/data/hke_stock.json")]
    else:
        raise ValueError("请输入正确的股票代码（A股6位或港股5位）")
    
    for market, url in urls:
        try:
            resp = requests.get(url, timeout=10)
            data = resp.json()["stockList"]
            for item in data:
                if item["code"] == stock_code:
                    return item["orgId"]
        except Exception:
            continue
    raise RuntimeError("未找到该股票的orgId，请检查代码是否正确。")

if __name__ == "__main__":
    # stock_code = input("请输入股票代码（A股6位或港股5位）: ").strip()
    parser = argparse.ArgumentParser(description="Baostock 股票数据分组下载器")
    parser.add_argument("group_index", type=int, help="组索引")
    parser.add_argument("total_groups", type=int, help="总组数")
    args = parser.parse_args()
    paths = get_paths()
    
    os.makedirs(paths["output_dir"], exist_ok=True)
    my_codes = get_stock_list(paths["finace_stock_path"], args.group_index, args.total_groups)
    print(f"正在处理 {len(my_codes)} 个股票代码...")
    # my_codes = my_codes[:1]  # 测试阶段先限制数量，正式运行时注释掉
    for stock_code in my_codes:
        print(f"正在处理 {stock_code}...")
        try:
            org_id = get_org_id(stock_code)
            # download_dir = f"./{stock_code}_annual_reports"  # 临时目录名，会在获取到股票名称后更新
            download_dir = os.path.join(paths["output_dir"])
            print(f"开始下载 {stock_code} 的年度报告...")
            fetch_cninfo_annual_reports(stock_code, org_id, download_dir)
        except Exception as e:
            print(f"错误: {e}")
     